package pocketmath;

import com.zaxxer.hikari.HikariDataSource;

import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;

import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;

import lombok.extern.slf4j.Slf4j;

import static pocketmath.Utilities.md5;

@Slf4j
public class NeonApplication {
    static final byte[] someLongText = ("asdfkjhasdnklaubserfkn akv.h iuhLIUGY E8O7Y O87Y9PHE UH A98YLIUHK.JA" +
            "SDLIFH*&(*KLJN IUH FDJNKL USH DCIN IUH IY9HO87AWH EFIHUH ASDJC IUAHW EOF7 HASDICNB" +
            " IJSDN CUHA IEOFHB ASIDUH CVAIULSB FLIUHA SDIUHVC AIUSDB CVO87ASGHD GHH)Y O*& O87Y" +
            " 9FHU PA9SD8H F9A8SH EFIUHA SL*& (H H A9P8FH P89SH DF98AHS P9F8 HAS9D8F HJN KJHU " +
            "(*H AS9DFP8YAS9PDFASDFADSFASDKJFH AS;DJIF SADJ FLKASJ DFLKJSA D;FIJ A;OSIDJF OISAJD FOISA" +
            "ASDJF OISAJD F;AJSD F98AJ8UJ9   09j ;oij ;oij OIJ OIJ ;OIJ SDF;OIJMo; ij;oianmsd ;ofij ;o").getBytes(StandardCharsets.UTF_8);

    public static void main(String[] args) throws Exception {
        HikariDataSource dataSource = new HikariDataSource();

        dataSource.setJdbcUrl("jdbc:mysql://neon-performance-test.cic9c2z7qscq.us-east-1.rds.amazonaws.com/neon?rewriteBatchedStatements=true");
        dataSource.setDriverClassName("com.mysql.jdbc.Driver");
        dataSource.addDataSourceProperty("user", "admin");
        dataSource.addDataSourceProperty("password", "08d1fbe846d35a03");
        dataSource.setMaximumPoolSize(80);
        dataSource.setIdleTimeout(20);
        dataSource.setConnectionTimeout(300000);
        dataSource.setTransactionIsolation("TRANSACTION_READ_COMMITTED");

        JdbcTemplate jdbc = new JdbcTemplate(dataSource);

        final int numThread = Integer.parseInt(args[0]);

        ExecutorService executorService = Executors.newFixedThreadPool(numThread);

        final int[] fixedEventsSize = new int[] {100, 150, 200, 350, 999, 1001, 1302, 1700, 1987, 2313, 8192, 12036};

        for (int eventSize: fixedEventsSize) {
            double[] accumulated = new double[11];
            final ArrayList<Future<double[]>> result = new ArrayList<>(numThread);

            for (int i = 0; i < numThread; i++) {
                result.add(executorService.submit(new Task(jdbc, eventSize)));
            }

            for (Future<double[]> f : result) {
                double[] r = f.get();
                for (int i = 1; i < 11; i++) {
                    accumulated[i] += r[i];
                }
            }

            for (int i = 1; i < 11; i++) {
                log.info("Average step {} is {} for eventSize={}", i * 100, accumulated[i] / numThread, eventSize);
            }

            log.info("==========================================================================================");
            log.info("\n");
        }

        executorService.shutdown();
    }

    private static final class Task implements Callable<double[]> {
        final JdbcTemplate jdbcTemplate;
        final int eventSize;

        public Task(JdbcTemplate jdbcTemplates, int eventSize) {
            this.jdbcTemplate = jdbcTemplates;
            this.eventSize = eventSize;
        }

        @Override
        public double[] call() throws Exception {
            ThreadLocalRandom random = ThreadLocalRandom.current();

            double[] resultArray = new double[11];

            // trigger connection
            jdbcTemplate.update("INSERT INTO bid_details_409016" + " (`KEY`, `VALUE`) " +
                    "VALUES (1, 2) " +
                    "ON DUPLICATE KEY UPDATE `VALUE` = VALUES(`VALUE`)");

            for (int step = 1; step < 11; step++) {
                Thread.sleep(random.nextLong(50));
                List<String> list = new ArrayList<>();

                for (int i = 0; i < eventSize; i++) {
                    list.add(Integer.valueOf(random.nextInt()).toString() + Integer.valueOf(random.nextInt()).toString());
                }

                final int step_f = step * 100;

                final long startNano = System.nanoTime();
                int startIndex = 0;

                while (startIndex < eventSize) {
                    int fromIndex = startIndex;
                    int toIndex = Math.min(fromIndex + step_f, eventSize);
                    List<String> subList = list.subList(fromIndex, toIndex);

                    jdbcTemplate.batchUpdate(
                            "INSERT INTO bid_details_409016" + " (`KEY`, `VALUE`) " +
                                    "VALUES (?, ?) " +
                                    "ON DUPLICATE KEY UPDATE `VALUE` = VALUES(`VALUE`)",
                            new BatchPreparedStatementSetter() {
                                @Override
                                public void setValues(PreparedStatement ps, int i) throws SQLException {
                                    byte[] encrypted = md5(subList.get(i));
                                    ps.setBytes(1, encrypted);
                                    ps.setBytes(2, someLongText);
                                }

                                @Override
                                public int getBatchSize() {
                                    return subList.size();
                                }
                            }
                    );
                    startIndex = toIndex;
                }

                resultArray[step] = (System.nanoTime() - startNano) / 1_000_000D;
            }

            return resultArray;
        }
    }
}
