package Map;

/**
 * 
 * @Author joe
 * @Date 2017-04-05 15:51:53
 */

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class mobile {
	/*
	 * 鎻愬彇鏁版嵁
	 */

	public static class mobileMapper extends
			Mapper<LongWritable, Text, NullWritable, Text> {
		ArrayList<Double> polygonXA = new ArrayList<Double>();
		ArrayList<Double> polygonYA = new ArrayList<Double>();

		// private double N = 0;
		// private double M = 0;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			Configuration conf = context.getConfiguration();
			// N = conf.getDouble("XA", -1);// 璁块棶鍏变韩淇℃伅
			// M = conf.getDouble("YA", -1);// 璁块棶鍏变韩淇℃伅
			String str = conf.get("XA");
			String str1 = conf.get("YA");
			for (String s : str.split(",")) {
				polygonXA.add(Double.parseDouble(s));

			}
			for (String s : str1.split(",")) {
				polygonYA.add(Double.parseDouble(s));

			}

		}

		Test t = new Test();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String line = new String(value.getBytes(), 0, value.getLength(),
					"GBK");
			String[] valArr = line.toString().split("\\,");

			if (valArr.length > 11) {

				// String IMSI = valArr[0];

				// String IMEI = valArr[1];

				// String TimeStamp = valArr[2];
				// String LAC = valArr[3];
				String N = valArr[5];//39
				String E = valArr[4];//116

				// String EventID = valArr[6];
				// String Stat = valArr[7];
				// String Flag = valArr[8];
				// String SISDN = valArr[9];
				// String Type = valArr[10];
				// String Name = valArr[11];

				double py = Double.parseDouble(N);//39
				double px = Double.parseDouble(E);//116

				/**
				 * 浠ヤ笅鏄尯鍩熻寖鍥寸殑缁忕含搴︼紝淇敼鍗冲彲
				 */

				if (t.isPointInPolygon(px, py, polygonXA, polygonYA)) {
					// System.out.println("joe");
					context.write(NullWritable.get(), new Text(value));

				}
			}
		}
	}

	/*
	 * 缁熻鏁版嵁
	 */
	public static class mobileReduce extends
			Reducer<NullWritable, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text v2 : values) {
				context.write(key, v2);
			}
		}

	}

	/**
	 * main 鏂规硶
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
//		if (args.length == 0) {
//			System.out.println("杈撳叆鏍煎紡 锛歨adoop jar 鍖哄煙鍚嶅瓧 ");
//			return;
//		}
		String regionName = args[0];

		ArrayList<Double> polygonXA = new ArrayList<Double>();
		ArrayList<Double> polygonYA = new ArrayList<Double>();
		 File file = new File("D:\\xc.txt" + File.separator);// windows
		// File file = new File("/root/test.txt");// linux
		// File file = new File("/home/cyhp/test.txt" + File.separator);// linux

		//File file = new File("/root/test.txt" + File.separator);// linux
		
	 // File file = new File("/root/" + regionName + ".txt"+ File.separator);// linux
		
		
	

		// File file = new File("D:\\" + regionName + ".txt"+ File.separator);//

		InputStream is = new FileInputStream(file);// 鍒涘缓鏂囦欢杈撳叆娴�
		InputStreamReader isReader = new InputStreamReader(is);// 鍒涘缓杈撳嚭娴佺殑Reader
		BufferedReader reader = new BufferedReader(isReader);// 鐢ㄤ簬鎸夎璇诲彇鏂囦欢

		String txtLine = null;// 涓存椂鍙橀噺锛屼繚瀛樻寜琛岃鍙栧埌鐨勫瓧绗︿覆
		String splitRegexp = "(\\d*)(\\.)(\\d*)(\\,)(\\d*)(\\.)(\\d*)";
		Pattern pattern = Pattern.compile(splitRegexp);// 缂栬瘧姝ｅ垯
		Matcher matcher = null;// 姝ｅ垯鐨凪atcher瀵硅薄

		while ((txtLine = reader.readLine()) != null) {// 鎸夎璇诲彇鏂囦欢

			matcher = pattern.matcher(txtLine);
			while (matcher.find()) {// 鏌ユ壘绗﹀悎鏉′欢鐨勫尮閰嶇粍锛屽皢缁撴灉鏀惧叆List
				polygonXA.add(Double.valueOf(matcher.group(1)
						+ matcher.group(2) + matcher.group(3)));
				polygonYA.add(Double.valueOf(matcher.group(5)
						+ matcher.group(6) + matcher.group(7)));

			}
		}
		reader.close();
		System.out.println(polygonYA + "lol");
		System.out.println(polygonXA + "haha");
//		  polygonYA.add(39.902668);   
//		  polygonYA.add(39.90306);   
//		  polygonYA.add(39.905009);   
//		  polygonYA.add(39.943909);
//		  polygonYA.add(39.94853);   
//		  polygonYA.add(39.948896);   
//		  polygonYA.add(39.956984);   
//		  polygonYA.add(39.95056);
//		  polygonYA.add(39.95514);   
//		  polygonYA.add(39.973952);   
//		  polygonYA.add(39.974743);   
//		  polygonYA.add(39.978864);
//		  polygonYA.add(39.979423);   
//		  polygonYA.add(39.963351);   
//		  polygonYA.add(39.963746);   
//		  polygonYA.add(39.935055);
//		  polygonYA.add(39.934817);
//		  polygonYA.add(39.934817);
//		  polygonYA.add(39.93005);
//		  polygonYA.add(39.928921);
//		  polygonYA.add(39.918241);
//		  polygonYA.add(39.905052);
//		  polygonYA.add(39.878744);
//		  polygonYA.add(39.874923);
//		  polygonYA.add(39.87934);
//		  polygonYA.add(39.881857);
//		  polygonYA.add(39.881334);
//		  polygonYA.add(39.902668);
//		  
//		  polygonXA.add(116.332569);
//		  polygonXA.add(116.34511);
//		  polygonXA.add(116.341928);
//		  polygonXA.add(116.340057);
//		  polygonXA.add(116.33564);
//		  polygonXA.add(116.35807);
//		  polygonXA.add(116.36025);
//		  polygonXA.add(116.363765);
//		  polygonXA.add(116.378694);
//		  polygonXA.add(116.377076);
//		  polygonXA.add(116.387074);
//		  polygonXA.add(116.387728);
//		  polygonXA.add(116.400552);
//		  polygonXA.add(116.393121);
//		  polygonXA.add(116.400071);
//		  polygonXA.add(116.403096);
//		  polygonXA.add(116.405804);
//		  polygonXA.add(116.405983);
//		  polygonXA.add(116.398778);
//		  polygonXA.add(116.397988);
//		  polygonXA.add(116.404329);
//		  polygonXA.add(116.405568);
//		  polygonXA.add(116.357438);
//		  polygonXA.add(116.355858);
//		  polygonXA.add(116.348724);
//		  polygonXA.add(116.327981);
//		  polygonXA.add(116.332569);
		String outputpath = "hdfs://172.30.10.229:9000/data6/out";
		Configuration conf = new Configuration();
		conf.set("XA", polygonXA.toString().replaceAll("[\\[\\] ]", ""));
		conf.set("YA", polygonYA.toString().replaceAll("[\\[\\] ]", ""));
		Path path = new Path(outputpath);
		FileSystem fileSystem = path.getFileSystem(conf);
		if (fileSystem.isDirectory(path)) {
			fileSystem.delete(path, true);
		}
		Job job = Job.getInstance(conf);
		job.setJarByClass(mobile.class);
		conf.set("mapred.textoutputformat.separator", ",");

		// 鍒涘缓Job瀵硅薄
		FileInputFormat.addInputPath(job, new Path(
				"hdfs://172.30.10.229:9000/population/phone/beijing/20150823/*"));
		FileOutputFormat.setOutputPath(job, new Path(outputpath));

		job.setMapperClass(mobileMapper.class);
		job.setReducerClass(mobileReduce.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		// 璁剧疆mapper銆乺educe鐨勮緭鍑虹被鍨�
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.waitForCompletion(true);

	}

}
