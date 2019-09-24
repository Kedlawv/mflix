package mflix.lessons;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.ArrayList;
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


}
