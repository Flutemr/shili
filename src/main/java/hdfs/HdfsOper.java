package hdfs;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

public class HdfsOper {
	//对hfds的一个抽象
	FileSystem fs = null;
	
	/**
	 * 测试其他代码的时候会先运行before方法
	 */
	@Before
	public void getFs(){
		//1、获取具体的hdfs实例
		try {
			fs = FileSystem.get(new URI("hdfs://flutemr.com:8020"),
							    new Configuration(),
							    "hello");
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
	}
	@Test
	/**
	 * 从hdfs中下载一个文件到本地
	 * hadoop common 底层封装了hdfs nio流
	 */
	public void testDownload() throws IllegalArgumentException, IOException{
		//通过nio fsdatainputstream 将hdfs文件先读到内存中
		FSDataInputStream in = fs.open(new Path("/input/file.log"));
		FileOutputStream out = new FileOutputStream("D:/bigdata/aa.txt");
		IOUtils.copyBytes(in, out, 4096, true);
		
	}
	/**
	 * 测试创建文件夹
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 */
	@Test	
	public void testMkdir() throws IllegalArgumentException, IOException{
		boolean issuccess = fs.mkdirs(new Path("/input/mkdir"));
		System.out.println(issuccess);
	
	}
	//测试上传
		@Test
		public  void testUpload() throws Exception, IOException{
			//在HDFS上创建一个用来接收的目录
			FSDataOutputStream  out  = fs.create(new Path("/input/SparkRDD"), true) ;
			
			//将本地的文件上传到HDFS
			FileInputStream   in =  new FileInputStream(
					"C://Users//lx//Desktop//bb.txt"
					);
			
			IOUtils.copyBytes(in, out, 4096, true);
		}
}
