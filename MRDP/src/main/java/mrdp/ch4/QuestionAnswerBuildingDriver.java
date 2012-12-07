package mrdp.ch4;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.xml.sax.InputSource;

public class QuestionAnswerBuildingDriver {

	public static class PostCommentMapper extends
			Mapper<Object, Text, Text, Text> {

		private DocumentBuilderFactory dbf = DocumentBuilderFactory
				.newInstance();
		private Text outkey = new Text();
		private Text outvalue = new Text();

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			// Parse the post/comment XML hierarchy into an Element
			Element post = getXmlElementFromString(value.toString());

			int postType = Integer.parseInt(post.getAttribute("PostTypeId"));

			// If postType is 1, it is a question
			if (postType == 1) {
				outkey.set(post.getAttribute("Id"));
				outvalue.set("Q" + value.toString());
			} else {
				// Else, it is an answer
				outkey.set(post.getAttribute("ParentId"));
				outvalue.set("A" + value.toString());
			}

			context.write(outkey, outvalue);
		}

		private Element getXmlElementFromString(String xml) {
			try {
				// Create a new document builder
				DocumentBuilder bldr = dbf.newDocumentBuilder();

				// Parse the XML string and return the first element
				return bldr.parse(new InputSource(new StringReader(xml)))
						.getDocumentElement();
			} catch (Exception e) {
				return null;
			}
		}
	}

	public static class QuestionAnswerReducer extends
			Reducer<Text, Text, Text, NullWritable> {

		private ArrayList<String> answers = new ArrayList<String>();
		private DocumentBuilderFactory dbf = DocumentBuilderFactory
				.newInstance();
		private String question = null;

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// Reset variables
			question = null;
			answers.clear();

			// For each input value
			for (Text t : values) {
				// If this is the post record, store it, minus the flag
				if (t.charAt(0) == 'Q') {
					question = t.toString().substring(1, t.toString().length())
							.trim();
				} else {
					// Else, it is a comment record. Add it to the list, minus
					// the flag
					answers.add(t.toString()
							.substring(1, t.toString().length()).trim());
				}
			}

			// If post is not null
			if (question != null) {
				// nest the comments underneath the post element
				String postWithCommentChildren = nestElements(question, answers);

				// write out the XML
				context.write(new Text(postWithCommentChildren),
						NullWritable.get());
			}
		}

		private String nestElements(String post, List<String> comments) {
			try {
				// Create the new document to build the XML
				DocumentBuilder bldr = dbf.newDocumentBuilder();
				Document doc = bldr.newDocument();

				// Copy parent node to document
				Element postEl = getXmlElementFromString(post);
				Element toAddPostEl = doc.createElement("question");

				// Copy the attributes of the original post element to the new
				// one
				copyAttributesToElement(postEl.getAttributes(), toAddPostEl);

				// For each comment, copy it to the "post" node
				for (String commentXml : comments) {
					Element commentEl = getXmlElementFromString(commentXml);
					Element toAddCommentEl = doc.createElement("answer");

					// Copy the attributes of the original comment element to
					// the new one
					copyAttributesToElement(commentEl.getAttributes(),
							toAddCommentEl);

					// Add the copied comment to the post element
					toAddPostEl.appendChild(toAddCommentEl);
				}

				// Add the post element to the document
				doc.appendChild(toAddPostEl);

				// Transform the document into a String of XML and return
				return transformDocumentToString(doc);

			} catch (Exception e) {
				return null;
			}
		}

		private Element getXmlElementFromString(String xml) {
			try {
				// Create a new document builder
				DocumentBuilder bldr = dbf.newDocumentBuilder();

				// Parse the XML string and return the first element
				return bldr.parse(new InputSource(new StringReader(xml)))
						.getDocumentElement();
			} catch (Exception e) {
				return null;
			}
		}

		private void copyAttributesToElement(NamedNodeMap attributes,
				Element element) {

			// For each attribute, copy it to the element
			for (int i = 0; i < attributes.getLength(); ++i) {
				Attr toCopy = (Attr) attributes.item(i);
				element.setAttribute(toCopy.getName(), toCopy.getValue());
			}
		}

		private String transformDocumentToString(Document doc) {
			try {
				TransformerFactory tf = TransformerFactory.newInstance();
				Transformer transformer = tf.newTransformer();
				transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION,
						"yes");
				StringWriter writer = new StringWriter();
				transformer.transform(new DOMSource(doc), new StreamResult(
						writer));
				// Replace all new line characters with an empty string to have
				// one record per line.
				return writer.getBuffer().toString().replaceAll("\n|\r", "");
			} catch (Exception e) {
				return null;
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err
					.println("Usage: QuestionAnswerHierarchy <post-comment> <outdir>");
			System.exit(1);
		}

		Job job = new Job(conf, "QuestionAnswerHierarchy");
		job.setJarByClass(QuestionAnswerBuildingDriver.class);

		job.setMapperClass(PostCommentMapper.class);

		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.setInputPaths(job, new Path(otherArgs[0]));

		job.setReducerClass(QuestionAnswerReducer.class);

		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
