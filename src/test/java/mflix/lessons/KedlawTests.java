package mflix.lessons;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.model.*;
import mflix.api.daos.UserDao;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@SpringBootTest
public class KedlawTests extends AbstractLesson{


    @Test
    public void singleStagePipeline(){
        String country = "Poland";
        List<Document> polandMovies = new ArrayList<>();

        //build filter
        Bson countryFilter = Filters.eq("countries",country);
        //create a pipeline stage
        Bson matchStage = Aggregates.match(countryFilter);
        //create a list holding pipeline stages
        List<Bson> pipeline = new ArrayList<>();
        //add stage to pipeline;
        pipeline.add(matchStage);
        //call aggregate() on the collection,give pipeline as argument, returns AggregateIterable object
        //containing a list of Document
        AggregateIterable<Document>  aggregateIterable = moviesCollection.aggregate(pipeline);
        //assign Aggregate to a result list using into() of the AggregateIterable object
        aggregateIterable.into(polandMovies);


        Assert.assertEquals(256,polandMovies.size());
    }

    @Test
    public void multiStagePipeline(){
    String country = "Poland";
    List<Document> directrorsNoOfFilms = new ArrayList<>();

    Bson countryFilter = Filters.eq("countries",country);
    Bson matchStage = Aggregates.match(countryFilter);

    Bson unwindDirectorsStage = Aggregates.unwind("$directors");

    Bson projection = Projections.include("directors");
    Bson projectionStage = Aggregates.project(projection);

    //group by directors, creates a list of documents with directors as unique id fields
        // sums the duplicates and stores the result of the accumulation in the new document
        // in the count field
    Bson groupStage = Aggregates.group("$directors",
            Accumulators.sum("count",1));

    Bson sortStage = Aggregates.sort(Sorts.descending("count"));

    List<Bson> pipeline = new ArrayList<>();
    pipeline.add(matchStage);
    pipeline.add(projectionStage);
    pipeline.add(unwindDirectorsStage);
    pipeline.add(groupStage);
    pipeline.add(sortStage);

    AggregateIterable<Document> aggregateIterable = moviesCollection.aggregate(pipeline);

    for(Document doc : aggregateIterable){
        System.out.println(doc);
        directrorsNoOfFilms.add(doc);

    }
    Assert.assertEquals(141,directrorsNoOfFilms.size());
    Assert.assertEquals("Andrzej Wajda",directrorsNoOfFilms.get(0).get("_id"));



    }

    @Test
    public void testHowDocumentLooks(){
        Document appendCommentsStage = new Document("$lookup",
                new Document("from", "comments")
                        .append("let",
                                new Document("id", "$_id"))
                        .append("pipeline", Arrays.asList(new Document("$match",
                                        new Document("$expr",
                                                new Document("$eq", Arrays.asList("$movie_id", "$$id")))),
                                new Document("$sort",
                                        new Document("date", -1L))))
                        .append("as", "comments"));
        System.out.println(appendCommentsStage);

    }

}
