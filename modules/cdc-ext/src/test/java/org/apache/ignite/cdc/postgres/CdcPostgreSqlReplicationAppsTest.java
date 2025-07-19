package org.apache.ignite.cdc.postgres;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.sql.DataSource;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.startup.cmdline.CdcCommandLineStartup;

import static org.apache.ignite.cdc.kafka.CdcKafkaReplicationAppsTest.prepareConfig;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/** PostgreSql CDC test with .xml configuration. */
public class CdcPostgreSqlReplicationAppsTest extends CdcPostgreSqlReplicationTest {
    /** {@inheritDoc} */
    @Override protected IgniteInternalFuture<?> startIgniteToPostgreSqlCdcConsumer(
        IgniteConfiguration igniteCfg,
        Set<String> caches,
        DataSource dataSrc
    ) {
        String cfgPath = "/replication/ignite-to-postgres.xml";
        String threadName = "ignite-src-to-postgres-xml";

        Map<String, String> params = new HashMap<>();

        params.put("INSTANCE_NAME", igniteCfg.getIgniteInstanceName());
        params.put("CONSISTENT_ID", String.valueOf(igniteCfg.getConsistentId()));

        params.put("MAX_BATCH_SIZE", Integer.toString(MAX_BATCH_SIZE));
        params.put("ONLY_PRIMARY", "true");
        params.put("CREATE_TABLE", String.valueOf(createTables));

        params.put("DB_URL", postgres.getJdbcUrl("postgres", "postgres"));
        params.put("DB_USER", "postgres");
        params.put("DB_PASSWORD", "");

        return runAsync(() -> CdcCommandLineStartup.main(new String[] {prepareConfig(cfgPath, params)}), threadName);
    }

    /** {@inheritDoc} */
    @Override protected void checkFutureEndWithError(IgniteInternalFuture<?> fut) {
        // CdcMain error in CdcCommandLineStartup leads to 'System.exit(-1)' without showing error/cancellation in fut
    }
}
