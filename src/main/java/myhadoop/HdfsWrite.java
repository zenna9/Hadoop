package myhadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class HdfsWrite {
	//HdfsWrite <filename> <contents>
	public static void main(String[] args) {
		//�Է� �Ķ���� ����
		if (args.length <=2) { // �Ķ���Ͱ� ���ڶ�� ��
			System.err.println("Usage : HdfsWrite <filename> <contents>");
			System.exit(2);
		}
		
		try {
			//�ϵ� ���� �ҷ�����
			Configuration conf = new Configuration();
			//HDFS ���Ͻý��� �ҷ�����
			FileSystem fs = FileSystem.get(conf);
			
			//���� ��� üũ : args[0]
			Path path = new Path(args[0]);
			
			//���� ��ΰ� �̹� ������ overwrite�� �ȵǴϱ� �����ֱ��
			if (fs.exists(path)) { // �����ο� path�� �̹� �ִٸ�
				fs.delete(path, true); // fs���� path�� �����ֵ�, ��Ŀ�ú�� ����°� true
			}
			
			// �������� �ҷ��ͼ� args[1] ~args[args.lengh -1 ] 
			FSDataOutputStream os = fs.create(path);
			
			//������ �����͸� ���� ���� �о���� 
			for (int i =1 ; i< args.length ; i++) {
				//�ƿ�ǲ ��Ʈ���� ���
				os.writeUTF(args[i] + " ");
			}
			//���� ���
			os.writeUTF("\n");
			os.close();
			
			System.out.println("�޽����� ����Ǿ����ϴ�");
		} catch (IOException e) {
			System.err.println("���� ���� ����!");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}}

