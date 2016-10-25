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
		String key=null;
		List<String> chineselist=null;
		String decch=null;
		int num =200;
		try {
			if(redisServer!=null){
				//对集合key进行排序
		        key="mfg4_"+tdate+"_ChineseSet";
				sortingParams.by("mfg4_"+tdate+"_Zh_*");
				sortingParams.desc();
				sortingParams.limit(0, num);//限定返回结果的数量
				chineselist=redisServer.redis_sort2(key, sortingParams);
				if(chineselist!=null&&chineselist.size()>0){
					for(int i=0;i<chineselist.size();i++)
					{
						decch=new String(Base64.decodeBase64(chineselist.get(i)),"UTF-8");
						key="mfg4_"+tdate+"_Zh_"+chineselist.get(i);
						System.out.println(chineselist.get(i)+"	"+decch+" "+redisServer.get(key));
					}
				}
			}

		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		//获取中文对应的base64编码
		//测试中文提取与统计长度
//		try {
//			String[] urllist={"范冰冰","宣传"};//"淘宝","唯品会","苏宁","京东","当当"
//			//测试url串1："/hm.gif?cc=0&ck=1&cl=24-bit&ds=720x1280&et=0&ja=0&ln=zh-CN&lo=0&lt=1452054716&nv=1&rnd=1052692563&si=cdf7b63861fb9e5aa11b9f3859918fac&st=3&su=http%3A%2F%2Fcommon.diditaxi.com.cn%2Fgeneral%2FwebEntry%3Fwx%3Dtrue%26code%3D01169203ae60e01df8320537bd1ecb5o%26state%3D123&v=1.1.22&lv=3&tt=%E7%B2%89%E8%89%B2%E6%98%9F%E6%9C%9F%E4%B8%89";
//			//测试url串2："/025A84D404EA4E5834979B8A356DB4FA53340640/%5Bwww.qiqipu.com%5D%CB%DE%B5%D0.BD1024%B8%DF%C7%E5%D6%D0%D3%A2%CB%AB%D7%D6.mp4";
//			//测试url串3："/17.gif?n_try=0&t_ani=554&t_liv=6379&t_load=-9508&etype=slide&page=detail&app=mediacy&browser=baidubox&phoneid=50206&tanet=3&taspeed=287&logid=11218310436162814452&os=&wd=%E5%B0%91%E5%A6%87%E8%81%8A%E5%BE%AE%E4%BF%A1%E5%8F%91%E6%AF%94%E7%9A%84%E5%9B%BE%E7%89%87&sid=2c3ec78c910929ab174688703d173c16754ac96a&sampid=50&spat=1-0-nj02-&group="
//			//url=java.net.URLDecoder.decode(url, "utf-8");
//			String res=null;
//			for(int i=0;i<urllist.length;i++)
//			{
//				res=Base64.encodeBase64URLSafeString(urllist[i].getBytes("UTF-8"));
//				key="mfg4_"+tdate+"_Zh_"+res;
//				decch=redisServer.get(key);
//				if(decch!=null)System.out.println(res+" "+urllist[i]+" "+decch);
//			}
//		} catch (Exception ex) {
//			//logger.info("Yunguan_G4JKtest execute error: "+ex.getMessage());
//		}
		
		//测试获取热门app，需要去除浏览器和其他的app统计
//		try {
//			if(redisServer!=null){
//				//对集合key进行排序
//				key="mfg4_"+tdate+"_AppidSet";
//				sortingParams.by("mfg4_"+tdate+"_AppUse_*");
//				sortingParams.desc();
//				sortingParams.limit(0, num);//限定返回结果的数量
//				chineselist=redisServer.redis_sort2(key, sortingParams);
//				if(chineselist!=null&&chineselist.size()>0){
//					for(int i=0;i<chineselist.size();i++)
//					{
//						key="mfg4_"+tdate+"_AppUse_"+chineselist.get(i);
//						decch=redisServer.get(key);
//						key="ref_wtag_"+chineselist.get(i);
//						System.out.println(chineselist.get(i)+"	"+redisServer.get(key)+"	"+decch);
//					}
//				}
//			}
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		redisServer.close();
	}
}
