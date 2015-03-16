package ru.iborisov.springbatch;

import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.fluent.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.item.ChunkOrientedTasklet;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.ItemPreparedStatementSetter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

@SpringBootApplication
@EnableBatchProcessing
public class Application {
    private static final Logger LOG = LoggerFactory.getLogger(Application.class);

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private JdbcTemplate jdbcTpl;

    private static final class Url {
        public Long id;
        public String url;

        public Url(Long id, String url) {
            this.id = id;
            this.url = url;
        }
    }

    private static final class RespCode {
        public Url url;
        public Integer respCode;

        public RespCode(Url url, Integer respCode) {
            this.url = url;
            this.respCode = respCode;
        }
    }

    private static final class Host {
        public Url url;
        public String host;

        public Host(Url url, String host) {
            this.url = url;
            this.host = host;
        }
    }


    @Bean
    @StepScope
    public ItemReader<Url> siteReader() {
        final JdbcCursorItemReader<Url> reader = new JdbcCursorItemReader<>();
        reader.setDataSource(jdbcTpl.getDataSource());
        reader.setSql("SELECT id, url FROM urls");
        reader.setRowMapper(new RowMapper<Url>() {
            @Override
            public Url mapRow(ResultSet rs, int i) throws SQLException {
                return new Url(rs.getLong("id"), rs.getString("url"));
            }
        });
        return reader;
    }

    @Bean
    public ItemProcessor<Url, RespCode> responseCodeProcessor() {
        return new ItemProcessor<Url, RespCode>() {
            @Override
            public RespCode process(Url url) throws Exception {
                final HttpResponse resp = Request.Head(url.url).execute().returnResponse();
                final int statusCode = resp.getStatusLine().getStatusCode();
                LOG.info("Status code = {} for URL: {}", statusCode, url);
                return new RespCode(url, statusCode);
            }
        };
    }

    @Bean
    public ItemWriter<RespCode> responseCodeWriter() {
        final JdbcBatchItemWriter<RespCode> writer = new JdbcBatchItemWriter<>();
        writer.setDataSource(jdbcTpl.getDataSource());
        writer.setSql("UPDATE urls SET resp_code = ? WHERE id = ? ");
        writer.setItemPreparedStatementSetter(new ItemPreparedStatementSetter<RespCode>() {
            @Override
            public void setValues(RespCode respCode, PreparedStatement ps) throws SQLException {
                ps.setInt(1, respCode.respCode);
                ps.setLong(2, respCode.url.id);
            }
        });
        return writer;
    }

    @Bean
    public ItemProcessor<Url, Host> hostProcessor() {
        return new ItemProcessor<Url, Host>() {
            @Override
            public Host process(Url url) throws Exception {
                final HttpResponse resp = Request.Head(url.url).execute().returnResponse();
                final Header hostHeader = resp.getFirstHeader("Host");
                final String host = hostHeader != null ? hostHeader.getValue() : null;
                LOG.info("Host header = '{}' for URL: {}", host, url.url);
                return new Host(url, host);
            }
        };
    }

    @Bean
    public ItemWriter<Host> hostWriter() {
        final JdbcBatchItemWriter<Host> writer = new JdbcBatchItemWriter<>();
        writer.setDataSource(jdbcTpl.getDataSource());
        writer.setSql("UPDATE urls SET host = ? WHERE id = ?");
        writer.setItemPreparedStatementSetter(new ItemPreparedStatementSetter<Host>() {
            @Override
            public void setValues(Host host, PreparedStatement ps) throws SQLException {
                ps.setString(1, host.host);
                ps.setLong(2, host.url.id);
            }
        });
        return writer;
    }


    @Bean
    public Step responseCodeStep() {
        return stepBuilderFactory.get("responseCodeStep")
                .<Url, RespCode>chunk(1)
                .reader(siteReader())
                .processor(responseCodeProcessor())
                .writer(responseCodeWriter())
                .faultTolerant()
                .retry(Exception.class)
                .retryLimit(3)
                .build();
    }

    @Bean
    Step hostStep() {
        return stepBuilderFactory.get("hostStep")
                .<Url, Host>chunk(1)
                .reader(siteReader())
                .processor(hostProcessor())
                .writer(hostWriter())
                .faultTolerant()
                .retry(Exception.class)
                .retryLimit(3)
                .build();
    }

    @Bean
    Step noopStep() {
        return stepBuilderFactory.get("noopStep")
                .tasklet(new Tasklet() {
                    @Override
                    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
                        LOG.info("Inside noop");
                        return null;
                    }
                })
                //.<Url, Url>chunk(1)

                .build();
    }

    @Bean
    public Job job() throws Exception {
        final ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setCorePoolSize(5);
        taskExecutor.setMaxPoolSize(10);
        taskExecutor.setQueueCapacity(50);
        taskExecutor.initialize();

//        SimpleFlow splitFlow = new FlowBuilder<SimpleFlow>("Split Flow")
//                .split(new SimpleAsyncTaskExecutor())
//                .add(flow2(), flow3())
//                .build();
//        return new FlowBuilder<SimpleFlow>("Main Flow")
//                .start(flow1())
//                .next(splitFlow)
//                .end();


//        new FlowBuilder<SimpleFlow>("splitFlow")
//                .split(jobTaskExecutor)
//                .add(flow1(), flow2(), flow3())
//                .end();
//
//
//        new FlowBuilder<SimpleFlow>("splitFlow")
//                .from(flow1())
//                .split(jobTaskExecutor)
//                .add(flow2(), flow3())
//                .end();


        final SimpleFlow splitFlow = new FlowBuilder<SimpleFlow>("splitFlow")
                .start(noopStep())
                .split(taskExecutor)
                .add(new FlowBuilder<SimpleFlow>("hostFlow").start(hostStep()).build(),
                        new FlowBuilder<SimpleFlow>("respCodeFlow").start(responseCodeStep()).build())
                .build();

        return jobBuilderFactory.get("job1")
                .incrementer(new RunIdIncrementer())
                .start(splitFlow)
                .build()
                .build();
    }

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }
}
