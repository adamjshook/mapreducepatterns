package mrdp.ch4;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import mrdp.utils.MRDPUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.xml.sax.InputSource;

public class PostCommentBuildingDriver {

	public static class PostMapper extends Mapper<Object, Text, Text, Text> {

		private Text outkey = new Text();
		private Text outvalue = new Text();

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			// Parse the input string into a nice map
			Map<String, String> parsed = MRDPUtils.transformXmlToMap(value
					.toString());

			String postId = parsed.get("Id");

			if (postId == null) {
				return;
			}

			// The foreign join key is the post ID
			outkey.set(postId);

			// Flag this record for the reducer and then output
			outvalue.set("P" + value.toString());
			context.write(outkey, outvalue);
		}
	}

	public static class CommentMapper extends Mapper<Object, Text, Text, Text> {
		private Text outkey = new Text();
		private Text outvalue = new Text();

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			// Parse the input string into a nice map
			Map<String, String> parsed = MRDPUtils.transformXmlToMap(value
					.toString());

			String postId = parsed.get("PostId");
			if (postId == null) {
				return;
			}

			// The foreign join key is the user ID
			outkey.set(postId);

			// Flag this record for the reducer and then output
			outvalue.set("C" + value.toString());
			context.write(outkey, outvalue);
		}
	}

	public static class PostCommentHierarchyReducer extends
			Reducer<Text, Text, Text, NullWritable> {

		private ArrayList<String> comments = new ArrayList<String>();
		private DocumentBuilderFactory dbf = DocumentBuilderFactory
				.newInstance();
		private String post = null;

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// Reset variables
			post = null;
			comments.clear();

			// For each input value
			for (Text t : values) {
				// If this is the post record, store it, minus the flag
				if (t.charAt(0) == 'P') {
					post = t.toString().substring(1, t.toString().length())
							.trim();
				} else {
					// Else, it is a comment record. Add it to the list, minus
					// the flag
					comments.add(t.toString()
							.substring(1, t.toString().length()).trim());
				}
			}

			// If post is not null
			if (post != null) {
				// nest the comments underneath the post element
				String postWithCommentChildren = nestElements(post, comments);

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
				Element toAddPostEl = doc.createElement("post");

				// Copy the attributes of the original post element to the new
				// one
				copyAttributesToElement(postEl.getAttributes(), toAddPostEl);

				// For each comment, copy it to the "post" node
				for (String commentXml : comments) {
					Element commentEl = getXmlElementFromString(commentXml);
					Element toAddCommentEl = doc.createElement("comments");

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
		if (otherArgs.length != 3) {
			System.err
					.println("Usage: PostCommentHierarchy <posts> <comments> <outdir>");
			System.exit(1);
		}

		Job job = new Job(conf, "PostCommentHierarchy");
		job.setJarByClass(PostCommentBuildingDriver.class);

		MultipleInputs.addInputPath(job, new Path(otherArgs[0]),
				TextInputFormat.class, PostMapper.class);

		MultipleInputs.addInputPath(job, new Path(otherArgs[1]),
				TextInputFormat.class, CommentMapper.class);

		job.setReducerClass(PostCommentHierarchyReducer.class);

		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		System.exit(job.waitForCompletion(true) ? 0 : 2);
	}
}
