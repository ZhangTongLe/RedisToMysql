package uNitTest;
//
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.Iterator;
//import java.io.UnsupportedEncodingException;
import java.util.List;
//import java.util.Map;
//import java.util.Set;
//import java.util.TreeSet;
//import java.util.TreeSet;

//import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;

import cm.redis.commons.RedisServer;
import cm.redis.commons.TimeFormatter;
import redis.clients.jedis.SortingParams;

public class RedisInstanceDataTest {
	public static void main(String[] args) {
		RedisServer redisServer=RedisServer.getInstance();
		String tdate=TimeFormatter.getDate2();
		String key=null;
		String decch=null;
		SortingParams sortingParams=new SortingParams();
		List<String> chineselist=null;
		int num = 500;
		
//		//数据扫描测试代码段
//		TreeSet<String> keys=null;
//		Iterator<String> keylist =null;
//		long num=0;
//		long tmp=0;
//		try{
//			//keys=redisServer.sscan("ref_hpm_set",null);  	//获取今天的全部热力基站点key信息
//			//if(keys!=null&&keys.size()>0){
//				num=0;
//				//keylist = keys.iterator();
//				//while(keylist.hasNext())
//				//{
//				//	tmp=0;
//				//	key=keylist.next().toString();
//					key="mfg4_"+tdate+"_custtag_1";//_hmset_15_00_
//					tmp=redisServer.scard(key);//获取对应的集合值
//					System.out.println(tmp);
//					
//					key="mfg4_"+tdate+"_custtag_2";//_hmset_15_00_
//					tmp=redisServer.scard(key);//获取对应的集合值
//					System.out.println(tmp);
//					
//					key="mfg4_"+tdate+"_custtag_3";//_hmset_15_00_
//					tmp=redisServer.scard(key);//获取对应的集合值
//					System.out.println(tmp);
//					
//					key="mfg4_"+tdate+"_custtag_4";//_hmset_15_00_
//					tmp=redisServer.scard(key);//获取对应的集合值
//					System.out.println(tmp);
//					
//					key="mfg4_"+tdate+"_custtag_5";//_hmset_15_00_
//					tmp=redisServer.scard(key);//获取对应的集合值
//					System.out.println(tmp);
//					
//					key="mfg4_"+tdate+"_custtag_6";//_hmset_15_00_
//					tmp=redisServer.scard(key);//获取对应的集合值
//					System.out.println(tmp);
//					
//					key="mfg4_"+tdate+"_custtag_7";//_hmset_15_00_
//					tmp=redisServer.scard(key);//获取对应的集合值
//					System.out.println(tmp);
//					
//					key="mfg4_"+tdate+"_custtag_8";//_hmset_15_00_
//					tmp=redisServer.scard(key);//获取对应的集合值
//					System.out.println(tmp);
//					
//					key="mfg4_"+tdate+"_custtag_9";//_hmset_15_00_
//					tmp=redisServer.scard(key);//获取对应的集合值
//					System.out.println(tmp);
//					
//					key="mfg4_"+tdate+"_custtag_10";//_hmset_15_00_
//					tmp=redisServer.scard(key);//获取对应的集合值
//					System.out.println(tmp);
//					
//				//	if(tmp>0)num+=tmp;
//				//}
//				
//			//}
//		}catch(Exception ex){
//			System.out.println(" Thread RedisInstanceDataTest crashes: "+ex.getMessage());
//		}
		
		//测试base64解码
//		String[] test=null;
//		String url=	"5omL5py6,5ZCM5Z-O";
//		String res=null;
//		
//		try {
//			test=url.split(",");
//			for(int i=0;i<test.length;i++)
//			{	
//				res = new String(Base64.decodeBase64(test[i]),"UTF-8");
//				System.out.println(res);
//			}
//			
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
		
		try {
			if(redisServer!=null){
				//对集合key进行排序
		        key="mfg4_"+tdate+"_AppidSet";//mfg4_EBusiSet,"mfg4_"+tdate+"_ChineseSet","mfg4_BaiduSet","mfg4_"+tdate+"_AppidSet","mfg4_"+tdate+"_IntidSet"
				sortingParams.by("mfg4_"+tdate+"_AppUse_*");//_Zh_*,mfg4_"+tdate+"_ebusiw_*,_baiduw_*,_AppUse_*,_IntidUse_*
				sortingParams.desc();
				sortingParams.limit(0, num);//限定返回结果的数量
				chineselist=redisServer.redis_sort2(key, sortingParams);
				if(chineselist!=null&&chineselist.size()>0){
					for(int i=0;i<chineselist.size();i++)
					{
						//System.out.println(chineselist.get(i));
						key="ref_wtag_"+chineselist.get(i);
						decch=redisServer.get(key);
						//System.out.println(decch);
//						decch=new String(Base64.decodeBase64(decch));
//						decch=decch.replace("男士", "");
//						decch=decch.replace("女士", "");
//						decch=decch.replace("联通", "");
//						decch=decch.replace("电信", "");
//						decch=decch.replace("旗舰店", "");
//						decch=decch.replace("官方", "");
						if(StringUtils.contains(decch, "游戏")==true||StringUtils.contains(decch, "视频")==true||StringUtils.contains(decch, "音频")==true){
							key="mfg4_"+tdate+"_AppUse_"+chineselist.get(i);//,_ebusiw_,_Zh_,_baiduw_,_AppUse_,_IntidUse_
							System.out.println((i+1)+":"+decch+":"+redisServer.get(key));//
						}
					}
				}
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		//获取中文对应的base64编码
		//测试中文提取与统计长度
//		try {
////			//String[] urllist={"陶喆"};//"淘宝","唯品会","苏宁","京东","当当"
////			//测试url串1："/hm.gif?cc=0&ck=1&cl=24-bit&ds=720x1280&et=0&ja=0&ln=zh-CN&lo=0&lt=1452054716&nv=1&rnd=1052692563&si=cdf7b63861fb9e5aa11b9f3859918fac&st=3&su=http%3A%2F%2Fcommon.diditaxi.com.cn%2Fgeneral%2FwebEntry%3Fwx%3Dtrue%26code%3D01169203ae60e01df8320537bd1ecb5o%26state%3D123&v=1.1.22&lv=3&tt=%E7%B2%89%E8%89%B2%E6%98%9F%E6%9C%9F%E4%B8%89";
////			//测试url串2："/025A84D404EA4E5834979B8A356DB4FA53340640/%5Bwww.qiqipu.com%5D%CB%DE%B5%D0.BD1024%B8%DF%C7%E5%D6%D0%D3%A2%CB%AB%D7%D6.mp4";
////			//测试url串3："/17.gif?n_try=0&t_ani=554&t_liv=6379&t_load=-9508&etype=slide&page=detail&app=mediacy&browser=baidubox&phoneid=50206&tanet=3&taspeed=287&logid=11218310436162814452&os=&wd=%E5%B0%91%E5%A6%87%E8%81%8A%E5%BE%AE%E4%BF%A1%E5%8F%91%E6%AF%94%E7%9A%84%E5%9B%BE%E7%89%87&sid=2c3ec78c910929ab174688703d173c16754ac96a&sampid=50&spat=1-0-nj02-&group="
////			
////			//cntaobao%E8%B4%A2%E8%BF%90%E9%80%9A18
//		    String url=java.net.URLDecoder.decode("/025A84D404EA4E5834979B8A356DB4FA53340640/%5Bwww.qiqipu.com%5D%CB%DE%B5%D0.BD1024%B8%DF%C7%E5%D6%D0%D3%A2%CB%AB%D7%D6.mp4", "gb2312");
//		    System.out.println(url);
////		    String guess="财运通18";
////		    guess=java.net.URLEncoder.encode(guess,"UTF-8");
//		    
////			String res=null;
////			for(int i=0;i<urllist.length;i++)
////			{
////				res=Base64.encodeBase64URLSafeString(urllist[i].getBytes("UTF-8"));
////				key="mfg4_"+tdate+"_Zh_"+res;
////				decch=redisServer.get(key);
////				if(decch!=null)System.out.println(res+" "+urllist[i]+" "+decch);
////			}
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
//						key=redisServer.get(key);
//						if(key.contains("金融"))System.out.println(chineselist.get(i)+"	"+key+"	"+decch);//
//					}
//				}
//			}
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		
		
		//测试获取单个号码的上网情况，路径信息
//		try {
//			String imsi="460002735213736";//"ref_imsiphn_460020170543524";13417014512
//			//460002735213501,cxb
//			//460002735213495,cyf
//			//460002735217343,zbj
//			//460002735213524,xxl
//			//460002735213491,xxy
//			//460002735213489,xh
//			//460002735213493,mpp
//			//460002735239825,bx
//			//460002735213736,cl
//			String phnum=null;
//			TreeSet<String> rtinfo=null;
//			if(redisServer!=null){
//				//检查对应的号码
//		        key="ref_imsiphn_"+imsi;
//				phnum=redisServer.get(key);
//				System.out.println(phnum);
//				
//				//号码检查通过执行获取隐私信息
//				if(phnum!=null&&phnum.length()==11){
//					key="mfg4_"+tdate+"_imsihot_"+imsi;
//					rtinfo=redisServer.sscan(key, null);
//					if(rtinfo!=null){
//						for(String tmp:rtinfo){
//							System.out.println(tmp);
//						}
//					}
//				}
//			}
//
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		
		RedisServer.close();
	}
}
