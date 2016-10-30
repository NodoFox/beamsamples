
import com.google.common.base.Strings;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.util.IOChannelFactory;
import org.apache.beam.sdk.util.IOChannelUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.io.File;
import java.io.IOException;

/**
 * Created on 10/30/16.
 */
public class CharacterCount {
    public interface CharacterCountOptions extends PipelineOptions {
        @Description("Path from where to read the input")
        @Default.String("src/main/resources/shakespeare.txt")
        String getInputFilePath();
        void setInputFilePath(String value);

        @Description("Path to write output")
        @Default.InstanceFactory(CharacterOutputFactory.class)
        String getOutputFilePath();
        void setOutputFilePath(String value);

        public class CharacterOutputFactory implements DefaultValueFactory<String> {

            @Override public String create(PipelineOptions options) {
                String tempLocation = options.getTempLocation();
                if (!Strings.isNullOrEmpty(tempLocation)) {
                    try {
                        IOChannelFactory factory = IOChannelUtils.getFactory(tempLocation);
                        String result = factory.resolve(tempLocation, "charcounts.txt");
                        return result;
                    } catch (IOException e) {
                        throw new RuntimeException(String.format("Failed to resolve temp location: %s", tempLocation));
                    }
                } else {
                    throw new IllegalArgumentException("Must specify --output or --tempLocation");
                }
            }
        }
    }
    public static class ExtractWordsFn extends DoFn<String, String> {
        @DoFn.ProcessElement
        public void processElement(ProcessContext c ) {
            if(c.element()!= null) {
                //String regex = "([^(\\W_)]+){2,}";
                String regex = "\\s";
                String[] words = c.element().split(regex);
                for(String word : words) {
                    if(word != null && !word.isEmpty()) {
                        c.output(word.trim());
                    }

                }
            }
        }
    }
    public static class WordLengthFn extends DoFn<String, Integer> {
        @DoFn.ProcessElement
        public void processElement(ProcessContext c ) {
            if(c.element() != null) {
                c.output(c.element().length());
            }
        }
    }
    public static class SumWordLength extends PTransform<PCollection<String>,PCollection<Integer>> {

        @Override public PCollection<Integer> apply(PCollection<String> lines) {
            PCollection<String> words = lines.apply(ParDo.of(new ExtractWordsFn()));
            PCollection<Integer> wordLengths = words.apply(ParDo.of(new WordLengthFn()));
            PCollection<Integer> sumWordLengths = wordLengths.apply(Sum.integersGlobally());
            return sumWordLengths;
        }
    }
    public static class IntegerToStringFn extends SimpleFunction<Integer, String> {

        @Override public String apply(Integer input) {
            return String.valueOf(input);
        }
    }
    public static void main(String[] args) {
        CharacterCountOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(CharacterCountOptions.class);
        System.out.println(new File("").getAbsolutePath());
        Pipeline pipeline = Pipeline.create(options);
        pipeline.apply("ReadLines",TextIO.Read.from(options.getInputFilePath()))
                .apply(new SumWordLength())
                .apply(Sum.integersGlobally()).apply(MapElements.via(new IntegerToStringFn()))
                .apply("WriteLines", TextIO.Write.to(options.getOutputFilePath()));
        pipeline.run();
    }
}
