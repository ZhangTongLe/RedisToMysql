package uNitTest;

import java.io.UnsupportedEncodingException;
import java.util.List;

import org.apache.commons.codec.binary.Base64;

import cm.redis.commons.RedisServer;
import cm.redis.commons.TimeFormatter;
import redis.clients.jedis.SortingParams;

public class RedisInstanceDataTest {
	public static void main(String[] args) {
		RedisServer redisServer=RedisServer.getInstance();
		SortingParams sortingParams=new SortingParams();
		String tdate=TimeFormatter.getDate2();
		String key="mfg4_"+tdate+"_ChineseSet";
		List<String> chineselist=null;
		String decch=null;
		int num =150;
		try {
			if(redisServer!=null){
				//对集合key进行排序
				sortingParams.by("mfg4_"+tdate+"_Zh_*");
				sortingParams.desc();
				sortingParams.limit(0, num);//限定返回结果的数量
				chineselist=redisServer.redis_sort2(key, sortingParams);
				if(chineselist!=null&&chineselist.size()>0){
					for(int i=0;i<chineselist.size();i++)
					{
						decch=new String(Base64.decodeBase64(chineselist.get(i)),"UTF-8");
						System.out.println(chineselist.get(i)+"	"+decch);
					}
				}
			}
			redisServer.close();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
