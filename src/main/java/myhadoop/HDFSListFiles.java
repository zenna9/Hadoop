package myhadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

public class HDFSListFiles {

	public static void main(String[] args) {
		if(args.length !=1) { //���ڷ� <dir>�ȳѾ��
			System.err.println("Usage: HDFSListFiles <dir>"); // ���� �ȳ�
			System.exit(2);
		}
		
		//���ڰ����� �Ѿ�� ���͸��� ã�Ƽ� ����(���͸�)���
		//���� �ҷ�����
		Configuration conf = new Configuration();
		
		// ���ڷ� �Ѿ�� dir�� Ȯ��
		String dir = args[0];
		
		try { //����ó��
			FileSystem fs = FileSystem.get(conf);
			RemoteIterator<LocatedFileStatus> it = fs.listFiles(new Path(dir), true);
		
			while(it.hasNext()) { // ó���� �׸��� �� �ִ���
				LocatedFileStatus status = it.next(); // ���� ���� ����
				// ���� ���
				System.out.printf("%s %s%n", status.isDirectory() ? "d": "f", status.getPath()); // %�� ����Ÿ�� �ǹ�
			}
			
		} catch (IOException e) {
			e.printStackTrace(); //���� ���� ���
		}
	}

}
