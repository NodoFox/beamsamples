import com.google.common.base.Strings;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
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

        @Description("Path to write output") @Default.InstanceFactory(CharacterOutputFactory.class)
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

    //Break a line into PCollection of Characters
    public static class CharacterBreakFn extends DoFn<String, Character> {
        @DoFn.ProcessElement public void processElement(ProcessContext c) {
            if (c.element() != null) {
                char[] letters = c.element().toCharArray();
                for (char letter : letters) {
                    c.output(letter);
                }
            }
        }
    }

    public static class SumCharacters extends PTransform<PCollection<String>, PCollection<KV<Character, Long>>> {
        @Override public PCollection<KV<Character, Long>> apply(PCollection<String> lines) {
            PCollection<Character> characters = lines.apply(ParDo.of(new CharacterBreakFn()));
            characters.setCoder(AvroCoder.of(Character.TYPE)); // set encoding for character type

            PCollection<KV<Character, Long>> characterCount = characters.apply(Count.<Character>perElement());
            return characterCount;
        }
    }

    public static String delim = " : ";

    public static class ToStringFn extends SimpleFunction<KV<Character, Long>, String> {

        @Override public String apply(KV<Character, Long> input) {
            return input.getKey() + delim + String.valueOf(input.getValue());
        }
    }

    public static void main(String[] args) {
        CharacterCountOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(CharacterCountOptions.class);
        System.out.println(new File("").getAbsolutePath());
        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply("ReadLines", TextIO.Read.from(options.getInputFilePath())).apply(new SumCharacters())
                .apply(MapElements.via(new ToStringFn()))
                .apply("WriteLines", TextIO.Write.to(options.getOutputFilePath()));
        pipeline.run();
    }
}
