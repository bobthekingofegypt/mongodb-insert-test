package org.bobstuff;

import com.google.common.collect.Lists;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClients;
import com.mongodb.client.model.IndexModel;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import org.bobstuff.models.MainThing;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.data.mongo.MongoDataAutoConfiguration;
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.*;

@SpringBootApplication(exclude = {
  MongoAutoConfiguration.class,
  MongoDataAutoConfiguration.class
})
public class Main implements CommandLineRunner {
    public static void main(String[] args) {
        System.exit(SpringApplication.exit(SpringApplication.run(Main.class, args)));
    }

    @Override
    public void run(String... args) throws Exception {
        var mongoUri = args[0];
        var socketCount = Integer.parseInt(args[1]);

        var total = 1000000;
        var entries = new ArrayList<MainThing>(total);
        for (var i = 0; i < total; i += 1) {
            entries.add(Models.createMainThing());
        }

        CodecRegistry pojoCodecRegistry =
                CodecRegistries.fromProviders(
                        PojoCodecProvider.builder().automatic(true).build
                                ());
        var codecRegistry =
                CodecRegistries.fromRegistries(MongoClientSettings.getDefaultCodecRegistry(),
                                               pojoCodecRegistry);
        var settings =
                MongoClientSettings.builder()
                                   .codecRegistry(codecRegistry)
                                   .applyToSocketSettings(builder -> {
                                       builder.connectTimeout(10, TimeUnit.SECONDS);
                                   })
                                   .applyConnectionString(
                                           new ConnectionString(mongoUri))
                                   .build();
        var client = MongoClients.create(settings);
        var database = client.getDatabase("test_data_mongo_issue");
        var collection = database.getCollection("mainthings", MainThing.class);

        var someIdIndex = new IndexModel(Indexes.ascending("someId"), new IndexOptions().unique(true));

        collection.createIndexes(
                Arrays.asList(someIdIndex));

        ExecutorService service = Executors.newFixedThreadPool(socketCount);
        CompletionService<Void> completionService =
                new ExecutorCompletionService<>(service);

        var size = entries.size() / socketCount + 1;
        var lists = Lists.partition(entries, size);
        if (lists.size() != socketCount) {
            throw new IllegalStateException();
        }

        var start = System.currentTimeMillis();

        for (var i = 0; i < socketCount; i += 1) {
            final var subItems = lists.get(i);
            System.out.printf("bulk loading in %s items%n", subItems.size());
            completionService.submit(() -> {
                var innerStart = System.currentTimeMillis();
                collection.insertMany(subItems);
                System.out.println("finished loader in " + (System.currentTimeMillis() - innerStart));
                return null;
            });
        }

        var finished = 0;
        try {
            while (finished != socketCount) {
                completionService.take().get();
                finished += 1;
            }
        } finally {
            service.shutdownNow();
        }
        System.out.println(System.currentTimeMillis() - start);
    }
}
