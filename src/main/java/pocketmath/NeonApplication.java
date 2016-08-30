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
import java.util.stream.Collectors;

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

    static int numLoop;

    public static void main(String[] args) throws Exception {
        final int numThread = Integer.parseInt(args[0]);
        final int batchSize = Integer.parseInt(args[1]);
        final int numEvent = Integer.parseInt(args[2]);
        numLoop = Integer.parseInt(args[3]);

        HikariDataSource dataSource = new HikariDataSource();

        dataSource.setJdbcUrl("jdbc:mysql://neon-performance-test.cic9c2z7qscq.us-east-1.rds.amazonaws.com/neon?rewriteBatchedStatements=true");
        dataSource.setDriverClassName("com.mysql.jdbc.Driver");
        dataSource.addDataSourceProperty("user", "admin");
        dataSource.addDataSourceProperty("password", "08d1fbe846d35a03");
        dataSource.setMaximumPoolSize((int) 1.5*numThread);
        dataSource.setIdleTimeout(numThread);
        dataSource.setConnectionTimeout(30000);
        dataSource.setTransactionIsolation("TRANSACTION_READ_COMMITTED");

        JdbcTemplate jdbc = new JdbcTemplate(dataSource);

        for (int i=0; i< 10*numThread; i++) {
            // trigger connection
            jdbc.update("INSERT INTO bid_details_500000" + " (`KEY`, `VALUE`) " +
                    "VALUES (1, 2) " +
                    "ON DUPLICATE KEY UPDATE `VALUE` = VALUES(`VALUE`)");
        }

        ExecutorService executorService = Executors.newFixedThreadPool(numThread);

        double accumulated = 0;
        final ArrayList<Future<Double>> result = new ArrayList<>(numThread);

        for (int i = 0; i < numThread; i++) {
            result.add(executorService.submit(new Task(jdbc, numEvent, batchSize)));
        }

        for (Future<Double> f : result) {
            accumulated += f.get();
        }

        log.info("BatchSize={}, eventSize={}, time={}", batchSize, numEvent, accumulated/numThread);

        executorService.shutdown();
    }

    private static final class Task implements Callable<Double> {
        final JdbcTemplate jdbcTemplate;
        final int eventSize;
        final int batchSize;

        public Task(JdbcTemplate jdbcTemplates, int eventSize, int batchSize) {
            this.jdbcTemplate = jdbcTemplates;
            this.eventSize = eventSize;
            this.batchSize = batchSize;
        }

        @Override
        public Double call() throws Exception {
            ThreadLocalRandom random = ThreadLocalRandom.current();

            long totalTime=0;

            for (int loop=0; loop < numLoop; loop++) {
                Thread.sleep(random.nextLong(4000));
                final long startNano = System.nanoTime();

                List<String> list = new ArrayList<>();

                for (int i = 0; i < eventSize; i++) {
                    list.add(Integer.valueOf(random.nextInt()).toString() + Integer.valueOf(random.nextInt()).toString());
                }

                int startIndex = 0;

                List<byte[]> byteList = list.stream().map(e -> md5(e)).sorted(
                        (o1, o2) -> {

                            for (int i = 0; i < o1.length; i++) {
                                if (o1[i] < o2[i]) {
                                    return -1;
                                } else if (o1[i] > o2[i]) {
                                    return 1;
                                }
                            }

                            return 0;
                        }
                ).collect(Collectors.toList());

                while (startIndex < eventSize) {
                    int fromIndex = startIndex;
                    int toIndex = Math.min(fromIndex + batchSize, eventSize);
                    List<byte[]> subList = byteList.subList(fromIndex, toIndex);

                    jdbcTemplate.batchUpdate(
                            "INSERT INTO bid_details_500000" + " (`KEY`, `VALUE`) " +
                                    "VALUES (?, ?) " +
                                    "ON DUPLICATE KEY UPDATE `VALUE` = VALUES(`VALUE`)",
                            new BatchPreparedStatementSetter() {
                                @Override
                                public void setValues(PreparedStatement ps, int i) throws SQLException {
                                    ps.setBytes(1, subList.get(i));
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

                totalTime += System.nanoTime() - startNano;
            }

            return totalTime/(1_000_000D * numLoop);
        }
    }
}
