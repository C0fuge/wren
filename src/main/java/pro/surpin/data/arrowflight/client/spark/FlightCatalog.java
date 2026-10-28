package pro.surpin.data.arrowflight.client.spark;

import pro.surpin.data.arrowflight.client.Client;
import pro.surpin.data.arrowflight.client.Configuration;
import pro.surpin.data.arrowflight.client.model.Table;
import pro.surpin.data.arrowflight.client.spark.security.FlightSessionTokens;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Read-only Spark catalog over the Flight SQL server's metadata: the server's schemas are Spark
 * namespaces (one level) and its tables are Spark tables read through the Flight connector.
 *
 * <p>Configured as {@code spark.sql.catalog.<name>=pro.surpin.data.arrowflight.client.spark.FlightCatalog}
 * with the connection options of {@code format("flight")} under that prefix
 * ({@code spark.sql.catalog.<name>.host}, {@code .port}, {@code .user}, {@code .password}, TLS,
 * Kerberos, ...). The other read options under the prefix ({@code partition.size},
 * {@code statistics.filterSelectivity}, ...) are defaults for every scan of the catalog's tables.
 * Namespaces and tables come from the Flight SQL {@code GetDbSchemas} and {@code GetTables}
 * commands, so the server's authorization decides what is visible. Names are matched exactly
 * (case-sensitive). Creating, altering, renaming or dropping namespaces and tables is not
 * supported.</p>
 */
public class FlightCatalog implements TableCatalog, SupportsNamespaces {
    //names that can go into the FROM clause without quoting
    private static final Pattern PLAIN_NAME = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
    //options that select a table: they never come from the catalog configuration
    private static final String TABLE = "table";

    private String name;
    private Configuration configuration;
    private Map<String, String> scanDefaults = Map.of();
    private String columnQuote = "";
    private String[] defaultNamespace = new String[0];

    /**
     * Reads the connection options and the scan defaults; no server call is made here.
     *
     * @param catalogName the catalog name used in SQL
     * @param options the options under {@code spark.sql.catalog.<name>.}
     */
    @Override
    public void initialize(String catalogName, CaseInsensitiveStringMap options) {
        this.name = catalogName;
        this.configuration = FlightSource.configuration(options);
        this.scanDefaults = scanDefaults(options);
        this.columnQuote = options.getOrDefault("column.quote", "");
        String schema = options.getOrDefault(FlightSource.KEY_DEFAULT_SCHEMA, "");
        this.defaultNamespace = schema.isEmpty() ? new String[0] : new String[] {schema};
    }

    /**
     * Returns the catalog name.
     *
     * @return the name given in {@link #initialize}
     */
    @Override
    public String name() {
        return this.name;
    }

    /**
     * Returns the server's {@code default.schema} when configured, otherwise no namespace.
     *
     * @return default namespace
     */
    @Override
    public String[] defaultNamespace() {
        return this.defaultNamespace.clone();
    }

    @Override
    public String[][] listNamespaces() {
        return schemas(null).stream().map(s -> new String[] {s}).toArray(String[][]::new);
    }

    @Override
    public String[][] listNamespaces(String[] namespace) throws NoSuchNamespaceException {
        requireNamespace(namespace);
        return new String[0][];
    }

    @Override
    public boolean namespaceExists(String[] namespace) {
        return namespace.length == 1 && schemas(namespace[0]).contains(namespace[0]);
    }

    @Override
    public Map<String, String> loadNamespaceMetadata(String[] namespace) throws NoSuchNamespaceException {
        requireNamespace(namespace);
        return Map.of();
    }

    @Override
    public void createNamespace(String[] namespace, Map<String, String> metadata) {
        throw readOnly("CREATE NAMESPACE");
    }

    @Override
    public void alterNamespace(String[] namespace, NamespaceChange... changes) {
        throw readOnly("ALTER NAMESPACE");
    }

    @Override
    public boolean dropNamespace(String[] namespace, boolean cascade) {
        throw readOnly("DROP NAMESPACE");
    }

    @Override
    public Identifier[] listTables(String[] namespace) throws NoSuchNamespaceException {
        requireNamespace(namespace);
        return tables(namespace[0], null, false).keySet().stream()
                .map(table -> Identifier.of(namespace, table))
                .toArray(Identifier[]::new);
    }

    @Override
    public org.apache.spark.sql.connector.catalog.Table loadTable(Identifier ident) throws NoSuchTableException {
        if (ident.namespace().length != 1) {
            throw new NoSuchTableException(ident);
        }
        String schema = ident.namespace()[0];
        Schema arrowSchema = tables(schema, ident.name(), true).get(ident.name());
        if (arrowSchema == null) {
            throw new NoSuchTableException(ident);
        }
        Table table = Table.forTable(qualifiedName(schema, ident.name()), this.columnQuote);
        table.setSparkSchema(SparkTypes.toSpark(arrowSchema));
        return new FlightTable(this.configuration, table, this.scanDefaults);
    }

    @Override
    public boolean tableExists(Identifier ident) {
        return ident.namespace().length == 1
                && tables(ident.namespace()[0], ident.name(), false).containsKey(ident.name());
    }

    @Override
    public org.apache.spark.sql.connector.catalog.Table createTable(Identifier ident, StructType schema,
            Transform[] partitions, Map<String, String> properties) {
        throw readOnly("CREATE TABLE");
    }

    @Override
    public org.apache.spark.sql.connector.catalog.Table alterTable(Identifier ident, TableChange... changes) {
        throw readOnly("ALTER TABLE");
    }

    @Override
    public boolean dropTable(Identifier ident) {
        throw readOnly("DROP TABLE");
    }

    @Override
    public void renameTable(Identifier oldIdent, Identifier newIdent) {
        throw readOnly("RENAME TABLE");
    }

    /**
     * Returns the catalog options that act as scan defaults: everything but the table selection
     * (connection options are harmless there, scans ignore them).
     *
     * @param options catalog options
     * @return lower-case keys to values
     */
    static Map<String, String> scanDefaults(CaseInsensitiveStringMap options) {
        Map<String, String> defaults = new HashMap<>();
        options.asCaseSensitiveMap().forEach((key, value) -> {
            String lower = key.toLowerCase(Locale.ROOT);
            if (!TABLE.equals(lower)) {
                defaults.put(lower, value);
            }
        });
        return Collections.unmodifiableMap(defaults);
    }

    /**
     * Builds the FROM-clause name of a server table; names that are not plain identifiers are
     * double-quoted (embedded quotes doubled).
     *
     * @param schema server schema
     * @param table server table
     * @return {@code schema.table}
     */
    static String qualifiedName(String schema, String table) {
        return quoteIfNeeded(schema) + "." + quoteIfNeeded(table);
    }

    /**
     * Quotes one name part when it is not a plain identifier.
     *
     * @param part schema or table name
     * @return the part, quoted when needed
     */
    static String quoteIfNeeded(String part) {
        return PLAIN_NAME.matcher(part).matches() ? part : "\"" + part.replace("\"", "\"\"") + "\"";
    }

    /**
     * Fails unless the namespace is one server schema that exists.
     *
     * @param namespace Spark namespace
     * @throws NoSuchNamespaceException when it is not
     */
    private void requireNamespace(String[] namespace) throws NoSuchNamespaceException {
        if (!namespaceExists(namespace)) {
            throw new NoSuchNamespaceException(namespace);
        }
    }

    /**
     * Lists the server's schemas ({@code GetDbSchemas}).
     *
     * @param exactName only this schema (exact match), or null for all
     * @return schema names in server order
     */
    private List<String> schemas(String exactName) {
        Client client = FlightSessionTokens.connect(this.configuration);
        FlightSqlClient sql = new FlightSqlClient(client.getFlightClient());
        List<String> names = new ArrayList<>();
        FlightInfo info = sql.getSchemas(null, exactName, client.callOptions());
        forEachRoot(client, info, root -> {
            VarCharVector vector = (VarCharVector) root.getVector("db_schema_name");
            for (int i = 0; i < root.getRowCount(); i++) {
                String schema = text(vector, i);
                if (exactName == null || exactName.equals(schema)) {
                    names.add(schema);
                }
            }
        });
        return names;
    }

    /**
     * Lists tables of one server schema ({@code GetTables}).
     *
     * @param schema server schema (exact)
     * @param exactTable only this table (exact match), or null for all
     * @param includeSchema whether to fetch each table's Arrow schema
     * @return table names to Arrow schemas (null values without {@code includeSchema})
     */
    private Map<String, Schema> tables(String schema, String exactTable, boolean includeSchema) {
        Client client = FlightSessionTokens.connect(this.configuration);
        FlightSqlClient sql = new FlightSqlClient(client.getFlightClient());
        Map<String, Schema> tables = new LinkedHashMap<>();
        // LIKE patterns: '_' in a name matches any character, so the answer is filtered exactly here.
        FlightInfo info = sql.getTables(null, schema, exactTable, null, includeSchema, client.callOptions());
        forEachRoot(client, info, root -> {
            VarCharVector schemas = (VarCharVector) root.getVector("db_schema_name");
            VarCharVector names = (VarCharVector) root.getVector("table_name");
            VarBinaryVector schemaBytes = includeSchema ? (VarBinaryVector) root.getVector("table_schema") : null;
            for (int i = 0; i < root.getRowCount(); i++) {
                String table = text(names, i);
                if (!schema.equals(text(schemas, i)) || (exactTable != null && !exactTable.equals(table))) {
                    continue;
                }
                tables.put(table, schemaBytes == null ? null
                        : Schema.deserializeMessage(ByteBuffer.wrap(schemaBytes.get(i))));
            }
        });
        return tables;
    }

    /**
     * Streams every endpoint of a metadata answer.
     *
     * @param client pooled client
     * @param info metadata flight info
     * @param consumer called for each batch
     */
    private static void forEachRoot(Client client, FlightInfo info, java.util.function.Consumer<VectorSchemaRoot> consumer) {
        for (FlightEndpoint endpoint : info.getEndpoints()) {
            try (FlightStream stream = client.openStream(endpoint)) {
                while (stream.next()) {
                    consumer.accept(stream.getRoot());
                }
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new IllegalStateException("Reading Flight metadata failed: " + e.getMessage(), e);
            }
        }
    }

    /**
     * Reads one UTF-8 value.
     *
     * @param vector string vector
     * @param index row
     * @return value, or null
     */
    private static String text(VarCharVector vector, int index) {
        return vector.isNull(index) ? null : new String(vector.get(index), StandardCharsets.UTF_8);
    }

    /**
     * Builds the error for an unsupported DDL statement.
     *
     * @param statement statement name
     * @return exception to throw
     */
    private UnsupportedOperationException readOnly(String statement) {
        return new UnsupportedOperationException(statement + " is not supported: Flight catalog '" + this.name
                + "' is read-only");
    }
}
