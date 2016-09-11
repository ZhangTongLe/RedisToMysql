package cm.redis.settomysql;

import java.io.File;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import org.apache.log4j.Logger;

import cm.redis.commons.RedisServer;
import cm.redis.commons.ResourcesConfig;
import cm.redis.commons.TimeFormatter;


/**
 * 定时将当天最新的redis数据推送到mysql中，实现数据的持久化
 * @author nicolashsu 2016-09-08
 *
 */
public class Redis_To_Mysql {

	public static Logger logger=Logger.getLogger(Redis_To_Mysql.class);
	
	/**
	 * 抓取每15分钟的热点区域人流量，4Ghttp流量使用量数据，推送到mysql的dtdb数据库的tb_mofang_hotspot_flow_today表格中
	 * 表格tb_mofang_hotspot_flow_today，字段data_time，id，day，hour，minute，people_cnt，net_flow
	 */
	public static void PersisHotspotClockInfo()
	{
		//从redis获取对应key集合相关参数
		RedisServer redisserver=null;
		TreeSet<String> keys=null; 
		Iterator<String> keylist =null;
		String[] keysplit=null;
		String key=null;
		String cdate=null;
		
		int num=0;//统计记录
		
		//推送的字段组合
		String data_time=null;
		String id=null;
		String day=null;
		String hour=null;
		String minute=null;
		long pcnt=0;
		String people_cnt=null;
		double dnflow=0.0;
		long lnflow=0;
		String net_flow=null;
		
		//形成数据文件参数
		String filepath=null;
		File file = null;
		FileWriter fw=null;
		
		//数据库操作相关参数
		Connection conn=null;
		String sql=null;
		String url=ResourcesConfig.MYSQL_SERVER_URL+"?user="+ ResourcesConfig.MYSQL_USER
				+"&password="+ResourcesConfig.MYSQL_PASSWD+"&characterEncoding=UTF8";
		Statement stmt=null;

		logger.info(" Start to get hotspot clock info redis-keys");
		try{
			//获取实例
			redisserver=RedisServer.getInstance();					
			cdate=TimeFormatter.getDate2();        							//获取当前日期YYYY-MM-DD
			keys=redisserver.keys("mfg4_"+cdate+"_hspset_*"); 			//获取当前日期YYYY-MM-DD对应所有imsi信息的keys
			num=0;//统计记录
			if(keys!=null&&keys.size()>0)
			{
				day=TimeFormatter.getDate(); 				//当天日期 YYYYMMDD，删除mysql对应的当天记录
				data_time=TimeFormatter.getNow(); 		//获取当前时间YYYY-MM-DD HH:mm:ss
				filepath=ResourcesConfig.SYN_SERVER_DATAFILE+"tb_mofang_hotspot_flow_today.txt";
				file = new File(filepath);
				if (!file.isDirectory()) { 
					fw=new FileWriter(file);
					fw.write("");
					keylist = keys.iterator();
					while(keylist.hasNext())
					{
						key=keylist.next().toString();
						keysplit=key.split("_");
						if(keysplit.length==6)
						{
							id=keysplit[5];  						//hotspotid
							hour=keysplit[3]; 						//hour
							minute=keysplit[4]; 					//minute
							pcnt=redisserver.scard(key);
							if(pcnt<0)pcnt=0;
							people_cnt=String.valueOf(pcnt); //people_cnt
							key="mfg4_"+cdate+"_hspflux_"+hour+"_"+minute+"_"+id;
							net_flow=redisserver.get(key);
							if(net_flow!=null)dnflow=Double.valueOf(net_flow);
							lnflow=(long)dnflow;
							net_flow=String.valueOf(lnflow);  //net_flow
							key="'"+data_time+"','"+id+"','"+day+"','"+hour+"','"+minute+"','"+people_cnt+"','"+lnflow+"'\n";
							fw.write(key);
							num=num+1;
						}
					}
					fw.close();
					logger.info(" Complete get hotspot clock info, get "+num+" records");
					if(num>0)//有数据存在才考虑进行数据库录入
					{
						Class.forName(ResourcesConfig.MYSQL_SERVER_DRIVER);
						conn=DriverManager.getConnection(url);
						stmt =conn.createStatement();
						sql="delete from tb_mofang_hotspot_flow_today where day='"+day+"'";
						stmt.execute(sql);
						sql="load data local infile '"+filepath+"' replace into table tb_mofang_hotspot_flow_today fields terminated by ',' enclosed by '\\'' lines terminated by '\\n'";
						stmt.execute(sql);
						conn.close();	
						logger.info(" Set hotspots clock info  into mysql ok");
					}
			    }
			}else{
				logger.info(" Can't get redis hotspot clock info keys.");	
			}
		} catch (Exception e) {
			logger.info(" Thread Flush_Redis_DB Pushing hotspots clock info to Mysql crashes: "+e.getMessage());
		}
		
		//释放内存
		redisserver=null;
		keys=null;
		keylist=null;
		keysplit=null;
		key=null;
		cdate=null;
		data_time=null;
		id=null;
		day=null;
		hour=null;
		minute=null;
		pcnt=0;
		people_cnt=null;
		dnflow=0.0;
		lnflow=0;
		num=0;
		net_flow=null;
		filepath=null;
		file=null;
		fw=null;
		
		conn=null;
		sql=null;
		stmt=null;
	}
	
	/**
	 * 抓取当天hotspotid对应的imsi集合全部信息到mysql的表中
	 * 表格，字段data_time，hotspotid，imsi
	 */
	public static void PersisHotspotImsiSet()
	{
		//从redis获取对应key集合相关参数
		RedisServer redisserver=null;
		TreeSet<String> keys=null; 
		Iterator<String> keylist =null;
		Set<String> imsiset=null;
		String key=null;
		String cdate=null;
		
		int num=0;//统计记录
		
		//推送的字段组合
		String data_time=null;
		String id=null;
		
		//形成数据文件参数
		String filepath=null;
		File file = null;
		FileWriter fw=null;
		
		//数据库操作相关参数
//		Connection conn=null;
//		String sql=null;
//		String url=ResourcesConfig.MYSQL_SERVER_URL+"?user="+ ResourcesConfig.MYSQL_USER
//				+"&password="+ResourcesConfig.MYSQL_PASSWD+"&characterEncoding=UTF8";
//		Statement stmt=null;
		
		logger.info(" Start to get hotspot imsi set redis-keys");
		try{
			//获取实例
			redisserver=RedisServer.getInstance();					
			cdate=TimeFormatter.getDate2();        							//获取当前日期YYYY-MM-DD
			keys=redisserver.keys("mfg4_"+cdate+"_hspdayset_*"); 		//获取当前日期YYYY-MM-DD累计的imsi信息的keys
			num=0;//统计记录
			if(keys!=null&&keys.size()>0)
			{
				data_time=TimeFormatter.getNow(); 							//获取当前时间YYYY-MM-DD HH:mm:ss
				filepath=ResourcesConfig.SYN_SERVER_DATAFILE+"tb_mofang_hotspot_imsi_today.txt";
				file = new File(filepath);
				if (!file.isDirectory()) { 
					fw=new FileWriter(file);
					fw.write("");
					keylist = keys.iterator();
					while(keylist.hasNext())
					{
						key=keylist.next().toString();
						if(key.length()>26)
						{
							id=key.substring(26); //获取hotspotid
							imsiset=redisserver.smembers(key);
							for(String imsi:imsiset){
								key="'"+data_time+"','"+id+"','"+imsi+"'\n";
								fw.write(key);
								num=num+1;
							}
						}
					}
					fw.close();
					logger.info(" Complete get hotspot imsi set, get "+num+" records");
//					if(num>0)//有数据存在才考虑进行数据库录入
//					{
//						Class.forName(ResourcesConfig.MYSQL_SERVER_DRIVER);
//						conn=DriverManager.getConnection(url);
//						stmt =conn.createStatement();
//						sql="delete from tb_mofang_hotspot_imsi_today";
//						stmt.execute(sql);
//						sql="load data local infile '"+filepath+"' replace into table tb_mofang_hotspot_imsi_today fields terminated by ',' enclosed by '\\'' lines terminated by '\\n'";
//						stmt.execute(sql);
//						conn.close();	
//						logger.info(" Set hotspots imsi set into mysql ok");
//					}
			    }
			}else{
				logger.info(" Can't get redis hotspot imsi set keys.");	
			}
		} catch (Exception e) {
			logger.info(" Thread Flush_Redis_DB Pushing hotspots info to Mysql crashes: "+e.getMessage());
		}
		
		//释放内存
		redisserver=null;
		keys=null;
		keylist=null;
		imsiset=null;
		key=null;
		cdate=null;
		data_time=null;
		id=null;
		num=0;
		filepath=null;
		file=null;
		fw=null;
		
//		conn=null;
//		sql=null;
//		stmt=null;
	}
	
	/**
	 * 统计标签对应的全天人流量，4Ghttp流量
	 * 表格，字段data_time，tag，people_cnt，net_flow
	 */
	public static void PersisTagsInfo()
	{
		//从redis获取对应key集合相关参数
		RedisServer redisserver=null;
		TreeSet<String> keys=null; 
		Iterator<String> keylist =null;
		String key=null;
		String cdate=null;
		
		int num=0;//统计记录
		
		//推送的字段组合
		String data_time=null;
		String tagid=null;
		long pcnt=0;
		String people_cnt=null;
		double dnflow=0.0;
		long lnflow=0;
		String net_flow=null;
		
		//形成数据文件参数
		String filepath=null;
		File file = null;
		FileWriter fw=null;
		
		//数据库操作相关参数
//		Connection conn=null;
//		String sql=null;
//		String url=ResourcesConfig.MYSQL_SERVER_URL+"?user="+ ResourcesConfig.MYSQL_USER
//				+"&password="+ResourcesConfig.MYSQL_PASSWD+"&characterEncoding=UTF8";
//		Statement stmt=null;

		logger.info(" Start to get Tag info redis-keys");
		try{
			//获取实例
			redisserver=RedisServer.getInstance();					
			cdate=TimeFormatter.getDate2();        							//获取当前日期YYYY-MM-DD
			keys=redisserver.keys("mfg4_"+cdate+"_tagset_*"); 			//获取当前日期YYYY-MM-DD对应所有imsi信息的keys
			num=0;//统计记录
			if(keys!=null&&keys.size()>0)
			{
				data_time=TimeFormatter.getNow(); 		//获取当前时间YYYY-MM-DD HH:mm:ss
				filepath=ResourcesConfig.SYN_SERVER_DATAFILE+"tb_mofang_tags_info_today.txt";
				file = new File(filepath);
				if (!file.isDirectory()) { 
					fw=new FileWriter(file);
					fw.write("");
					keylist = keys.iterator();
					while(keylist.hasNext())
					{
						key=keylist.next().toString();
						if(key.length()>23)
						{
							tagid=key.substring(23);  						//tagid
							pcnt=redisserver.scard(key);
							if(pcnt<0)pcnt=0;
							people_cnt=String.valueOf(pcnt); 			//people_cnt
							key="mfg4_"+cdate+"_tagflux_"+tagid;
							net_flow=redisserver.get(key);
							if(net_flow!=null)dnflow=Double.valueOf(net_flow);
							lnflow=(long)dnflow;
							net_flow=String.valueOf(lnflow);  			//net_flow
							key="'"+data_time+"','"+tagid+"','"+people_cnt+"','"+lnflow+"'\n";
							fw.write(key);
							num=num+1;
						}
					}
					fw.close();
					logger.info(" Complete get Tag info, get "+num+" records");
//					if(num>0)//有数据存在才考虑进行数据库录入
//					{
//						Class.forName(ResourcesConfig.MYSQL_SERVER_DRIVER);
//						conn=DriverManager.getConnection(url);
//						stmt =conn.createStatement();
//						sql="delete from tb_mofang_tags_info_today";
//						stmt.execute(sql);
//						sql="load data local infile '"+filepath+"' replace into table tb_mofang_tags_info_today fields terminated by ',' enclosed by '\\'' lines terminated by '\\n'";
//						stmt.execute(sql);
//						conn.close();	
//						logger.info(" Set hotspots Tag info into mysql ok");
//					}
			    }
			}else{
				logger.info(" Can't get redis Tag info keys.");	
			}
		} catch (Exception e) {
			logger.info(" Thread Flush_Redis_DB Pushing Tag info to Mysql crashes: "+e.getMessage());
		}
		
		//释放内存
		redisserver=null;
		keys=null;
		keylist=null;
		key=null;
		cdate=null;
		data_time=null;
		tagid=null;
		pcnt=0;
		people_cnt=null;
		dnflow=0.0;
		lnflow=0;
		num=0;
		net_flow=null;
		filepath=null;
		file=null;
		fw=null;
		
//		conn=null;
//		sql=null;
//		stmt=null;
	}
	
	/**
	 * 抓取每5分钟的热点区域人流量，【4Ghttp流量使用量数据，表中无数据】，推送到mysql的dtdb数据库的tb_mofang_heapmap_ref表格中
	 * 表格tb_mofang_heapmap_ref，字段data_time，tac，ci，cnt
	 */
	public static void PersisHeapMapClockInfo()
	{
		//从redis获取对应key集合相关参数
		RedisServer redisserver=null;
		TreeSet<String> keys=null; 
		Iterator<String> keylist =null;
		String[] keysplit=null;
		String key=null;
		String cdate=null;
		
		int num=0;//统计记录
		
		//推送的字段组合
		String data_time=null;
		String tac=null;
		String ci=null;
		String hour=null;
		String minute=null;
		long pcnt=0;
		String people_cnt=null;
//		double dnflow=0.0;
//		long lnflow=0;
//		String net_flow=null;
		
		//形成数据文件参数
		String filepath=null;
		File file = null;
		FileWriter fw=null;
		
		//数据库操作相关参数
		Connection conn=null;
		String sql=null;
		String url=ResourcesConfig.MYSQL_SERVER_URL+"?user="+ ResourcesConfig.MYSQL_USER
				+"&password="+ResourcesConfig.MYSQL_PASSWD+"&characterEncoding=UTF8";
		Statement stmt=null;

		logger.info(" Start to get heapmap clock info redis-keys");
		try{
			//获取实例
			redisserver=RedisServer.getInstance();					
			cdate=TimeFormatter.getDate2();        							//获取当前日期YYYY-MM-DD
			keys=redisserver.keys("mfg4_"+cdate+"_hmset_*"); 			//获取当前日期YYYY-MM-DD对应所有imsi信息的keys
			num=0;//统计记录
			if(keys!=null&&keys.size()>0)
			{
				key=keys.last();//从默认的升序排序中，拿到当前统计的最新时刻
				keysplit=key.split("_");
				hour=keysplit[3];
				minute=keysplit[4];
				
				data_time=TimeFormatter.getNow(); 		//获取当前时间YYYY-MM-DD HH:mm:ss
				filepath=ResourcesConfig.SYN_SERVER_DATAFILE+"tb_mofang_heapmap_ref.txt";
				file = new File(filepath);
				if (!file.isDirectory()) { 
					fw=new FileWriter(file);
					fw.write("");
					keylist = keys.iterator();
					while(keylist.hasNext())
					{
						key=keylist.next().toString();
						keysplit=key.split("_");
						if(keysplit.length==7)
						{
							tac=keysplit[5];  						//tac
							ci=keysplit[6];							//ci
							key="mfg4_"+cdate+"_hmset_"+hour+"_"+minute+"_"+tac+"_"+ci;
							pcnt=redisserver.scard(key);
							if(pcnt<0)pcnt=0;
							people_cnt=String.valueOf(pcnt); //people_cnt
//							key="mfg4_"+cdate+"_hmflux_"+hour+"_"+minute+"_"+tac+"_"+ci;
//							net_flow=redisserver.get(key);
//							if(net_flow!=null)dnflow=Double.valueOf(net_flow);
//							lnflow=(long)dnflow;
//							net_flow=String.valueOf(lnflow);  //net_flow
							key="'"+data_time+"','"+tac+"','"+ci+"','"+people_cnt+"'\n";//+"','"+lnflow
							fw.write(key);
							num=num+1;
						}
					}
					fw.close();
					logger.info(" Complete get heapmap clock info, get "+num+" records");
					if(num>0)//有数据存在才考虑进行数据库录入
					{
						Class.forName(ResourcesConfig.MYSQL_SERVER_DRIVER);
						conn=DriverManager.getConnection(url);
						stmt =conn.createStatement();
						sql="delete from tb_mofang_heapmap_ref";
						stmt.execute(sql);
						sql="load data local infile '"+filepath+"' replace into table tb_mofang_heapmap_ref fields terminated by ',' enclosed by '\\'' lines terminated by '\\n'";
						stmt.execute(sql);
						conn.close();	
						logger.info(" Set heapmap clock info into mysql ok");
					}
			    }
			}else{
				logger.info(" Can't get redis heapmap clock info keys.");	
			}
		} catch (Exception e) {
			logger.info(" Thread Flush_Redis_DB Pushing heapmap clock info to Mysql crashes: "+e.getMessage());
		}
		
		//释放内存
		redisserver=null;
		keys=null;
		keylist=null;
		keysplit=null;
		key=null;
		cdate=null;
		data_time=null;
		tac=null;
		ci=null;
		hour=null;
		minute=null;
		pcnt=0;
		people_cnt=null;
//		dnflow=0.0;
//		lnflow=0;
//		num=0;
//		net_flow=null;
		filepath=null;
		file=null;
		fw=null;
		
		conn=null;
		sql=null;
		stmt=null;
	}
		
	
	/**
	 * 主函数
	 * @param args
	 */
	public static void main(String[] args)
	{
		while(true)
		{
			try {
				Redis_To_Mysql.PersisHotspotClockInfo(); //推送每15分钟的热点区域人流量，4Ghttp流量使用量数据
				Redis_To_Mysql.PersisHotspotImsiSet();   //推送当天热点区域的imsi数据明细
				Redis_To_Mysql.PersisTagsInfo();   			//推送当天热点区域中标签的累计人流量，4Ghttp流量使用量数据
				Redis_To_Mysql.PersisHeapMapClockInfo(); //推送每5分钟的热力图人流量信息
				Thread.sleep(1000*60*5);
			} catch (InterruptedException e) {
				logger.info(" Thread Flush_Redis_DB crashes: "+e.getMessage());
			}
		}
	}
}
