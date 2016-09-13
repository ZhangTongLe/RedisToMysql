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
	 * 抓取每15分钟的热点区域对应的人流量，4Ghttp流量使用量数据，推送到mysql的dtdb数据库的tb_mofang_hotspot_flow_today表格中
	 * 表格tb_mofang_hotspot_flow_today，字段data_time，id，day，hour，minute，people_cnt，net_flow
	 * 
	 * 抓取每15分钟的热点区域标签对应的人流量，推送到mysql的dtdb数据库的tb_mofang_hotspot_flow_today_tag表格中
	 * 表格tb_mofang_hotspot_flow_today_tag，字段data_time，id，day，hour，minute，people_cnt，tag
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
		
		//推送的字段组合
		String data_time=null;
		String id=null;
		String day=null;
		String hour=null;
		String minute=null;
		String tag=null;
		long pcnt=0;
		String people_cnt=null;
		double dnflow=0.0;
		long lnflow=0;
		String net_flow=null;
		
		//形成数据文件参数
		String fileflowpath=null;
		String filetagpath=null;
		File fileflow = null;
		File filetag=null;
		FileWriter fwflow=null;
		FileWriter fwtag=null;
		
		int numflow=0;//统计记录
		int numtag=0;
		
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
			cdate=TimeFormatter.getDate2();        								//获取当前日期YYYY-MM-DD
			keys=redisserver.keys("mfg4_"+cdate+"_hspset_*"); 			//获取当前日期YYYY-MM-DD对应所有imsi信息的keys，已经排好序
			if(keys==null||keys.size()==0){
				cdate=TimeFormatter.getYestoday2();
				keys=redisserver.keys("mfg4_"+cdate+"_hspset_*");
			}
			if(keys!=null&&keys.size()>0)
			{
				day=TimeFormatter.getDate(); 				//当天日期 YYYYMMDD，删除mysql对应的当天记录
				data_time=TimeFormatter.getNow(); 		//获取当前时间YYYY-MM-DD HH:mm:ss
				
				fileflowpath=ResourcesConfig.SYN_SERVER_DATAFILE+"tb_mofang_hotspot_flow_today.txt";
				fileflow = new File(fileflowpath);
				
				filetagpath=ResourcesConfig.SYN_SERVER_DATAFILE+"tb_mofang_hotspot_flow_today_tag.txt";
				filetag = new File(filetagpath);
				
				if (fileflow.exists()==true && fileflow.isDirectory()==false 
				&&filetag.exists()==true && filetag.isDirectory()==false	) { 
					fwflow=new FileWriter(fileflow);
					fwflow.write("");
					fwtag=new FileWriter(filetag);
					fwtag.write("");
					
					keylist = keys.iterator();
					while(keylist.hasNext())
					{
						key=keylist.next().toString();
						keysplit=key.split("_");
						if(keysplit.length>=6)
						{
							id=keysplit[5];  							//hotspotid
							hour=keysplit[3]; 						//hour
							minute=keysplit[4]; 					//minute
							
							if(keysplit.length==6){
								key="mfg4_"+cdate+"_hspset_"+hour+"_"+minute+"_"+id;
								pcnt=redisserver.scard(key);
								if(pcnt<0)pcnt=0;
								
								key="mfg4_"+cdate+"_hspflux_"+hour+"_"+minute+"_"+id;
								net_flow=redisserver.get(key);
								if(net_flow!=null)dnflow=Double.valueOf(net_flow);
								else dnflow=0.0;
								lnflow=(long)dnflow;
								
								people_cnt=String.valueOf(pcnt); 	//people_cnt = pcnt
								lnflow=(long)dnflow;
								net_flow=String.valueOf(lnflow);  	//net_flow from dnflow
								
								key="'"+data_time+"','"+id+"','"+day+"','"+hour+"','"+minute+"','"+people_cnt+"','"+net_flow+"'\n";
								fwflow.write(key);
								numflow=numflow+1;
							}
							
							if(keysplit.length==7){
								tag=keysplit[6];						//tagid
								
								key="mfg4_"+cdate+"_hspset_"+hour+"_"+minute+"_"+id+"_"+tag;
								pcnt=redisserver.scard(key);
								if(pcnt<0)pcnt=0;
								
//								key="mfg4_"+cdate+"_hspflux_"+hour+"_"+minute+"_"+id+"_"+tag;
//								net_flow=redisserver.get(key);
//								if(net_flow!=null)dnflow=Double.valueOf(net_flow);
//								else dnflow=0.0;
//								lnflow=(long)dnflow;
								
								people_cnt=String.valueOf(pcnt); 	//people_cnt = pcnt
//								lnflow=(long)dnflow;
//								net_flow=String.valueOf(lnflow);  	//net_flow from dnflow
								
								key="'"+data_time+"','"+id+"','"+day+"','"+hour+"','"+minute+"','"+people_cnt+"','"+tag+"'\n";
								fwtag.write(key);
								numtag=numtag+1;
							}
						}
					}
					fwflow.close();
					fwtag.close();
					logger.info(" Complete get hotspot clock info, get "+numflow+" flow records and "+numtag+" tag records");
					Class.forName(ResourcesConfig.MYSQL_SERVER_DRIVER);
					conn=DriverManager.getConnection(url);
					stmt =conn.createStatement();
					if(numflow>0)	//有数据存在才考虑进行数据库录入
					{
						sql="delete from tb_mofang_hotspot_flow_today where day='"+day+"'";
						stmt.execute(sql);
						sql="load data local infile '"+fileflowpath+"' replace into table tb_mofang_hotspot_flow_today fields terminated by ',' enclosed by '\\'' lines terminated by '\\n'";
						stmt.execute(sql);
						logger.info(" Set hotspots flow clock info  into mysql ok");
					}
					if(numtag>0)	//有数据存在才考虑进行数据库录入
					{
						sql="delete from tb_mofang_hotspot_flow_today_tag where day='"+day+"'";
						stmt.execute(sql);
						sql="load data local infile '"+filetagpath+"' replace into table tb_mofang_hotspot_flow_today_tag fields terminated by ',' enclosed by '\\'' lines terminated by '\\n'";
						stmt.execute(sql);
						logger.info(" Set hotspots tag clock info  into mysql ok");
					}
					conn.close();
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
		tag=null;
		pcnt=0;
		people_cnt=null;
		dnflow=0.0;
		lnflow=0;
		numflow=0;//统计记录
		numtag=0;
		net_flow=null;
		fileflowpath=null;
		filetagpath=null;
		fileflow = null;
		filetag=null;
		fwflow=null;
		fwtag=null;
		
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
		Connection conn=null;
		String sql=null;
		String url=ResourcesConfig.MYSQL_SERVER_URL+"?user="+ ResourcesConfig.MYSQL_USER
				+"&password="+ResourcesConfig.MYSQL_PASSWD+"&characterEncoding=UTF8";
		Statement stmt=null;
		
		logger.info(" Start to get hotspot imsi set redis-keys");
		try{
			//获取实例
			redisserver=RedisServer.getInstance();					
			cdate=TimeFormatter.getDate2();        							//获取当前日期YYYY-MM-DD
			keys=redisserver.keys("mfg4_"+cdate+"_hspdayset_*"); 		//获取当前日期YYYY-MM-DD累计的imsi信息的keys
			num=0;//统计记录
			if(keys==null||keys.size()==0){
				cdate=TimeFormatter.getYestoday2();
				keys=redisserver.keys("mfg4_"+cdate+"_hspdayset_*");
			}
			if(keys!=null&&keys.size()>0)
			{
				data_time=TimeFormatter.getNow(); 							//获取当前时间YYYY-MM-DD HH:mm:ss
				filepath=ResourcesConfig.SYN_SERVER_DATAFILE+"tb_mofang_hotspot_detail.txt";
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
					if(num>0)//有数据存在才考虑进行数据库录入
					{
						Class.forName(ResourcesConfig.MYSQL_SERVER_DRIVER);
						conn=DriverManager.getConnection(url);
						stmt =conn.createStatement();
						sql="delete from tb_mofang_hotspot_detail";
						stmt.execute(sql);
						sql="load data local infile '"+filepath+"' replace into table tb_mofang_hotspot_detail fields terminated by ',' enclosed by '\\'' lines terminated by '\\n'";
						stmt.execute(sql);
						conn.close();	
						logger.info(" Set hotspots imsi set into mysql ok");
					}
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
		
		conn=null;
		sql=null;
		stmt=null;
	}
	
	/**
	 * 抓取热点区域上网标签对应的累计人流量，4Ghttp流量使用量数据，推送到mysql的dtdb数据库的tb_mofang_hotspot_cust_tag表格中
	 * 表格tb_mofang_hotspot_cust_tag，字段data_time，id，tag，k，v，tag=webtagpeople，webtagflux (mb)
	 */
	public static void PersisHotspotWebClockInfo()
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
		String tag=null;
		String k=null;
		String kchn=null;
		String v=null;
		long pcnt=0;
		double dnflow=0.0;
		long lnflow=0;
		
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

		logger.info(" Start to get hotspot webapp clock info redis-keys");
		try{
			//获取实例
			redisserver=RedisServer.getInstance();					
			cdate=TimeFormatter.getDate2();        								//获取当前日期YYYY-MM-DD
			keys=redisserver.keys("mfg4_"+cdate+"_hspwtagset_*"); 		//获取当前日期YYYY-MM-DD对应所有imsi信息的keys
			num=0;//统计记录
			if(keys==null||keys.size()==0){
				cdate=TimeFormatter.getYestoday2();
				keys=redisserver.keys("mfg4_"+cdate+"_hspwtagset_*");
			}
			if(keys!=null&&keys.size()>0)
			{
				data_time=TimeFormatter.getNow(); 		//获取当前时间YYYY-MM-DD HH:mm:ss
				filepath=ResourcesConfig.SYN_SERVER_DATAFILE+"tb_mofang_hotspot_cust_tag.txt";
				file = new File(filepath);
				if (!file.isDirectory()) { 
					fw=new FileWriter(file);
					fw.write("");
					keylist = keys.iterator();
					while(keylist.hasNext())
					{
						key=keylist.next().toString();
						keysplit=key.split("_");
						if(keysplit.length>=5)
						{
							id=keysplit[3];  						//hotspotid
							k=keysplit[4];							//k
							if(k.equals("game")){
								kchn="1_游戏";
							}else if(k.equals("soccomm")){
								kchn="2_社交";
							}else if(k.equals("instmsg")){
								kchn="3_即时通信";
							}else if(k.equals("travel")){
								kchn="4_旅游出行";
							}else if(k.equals("finance")){
								kchn="5_金融理财";
							}else if(k.equals("webbusi")){
								kchn="6_网络购物";
							}else if(k.equals("convlife")){
								kchn="7_便捷生活";
							}else if(k.equals("newsinfo")){
								kchn="8_新闻资讯";
							}else if(k.equals("tools")){
								kchn="9_工具";
							}else if(k.equals("read")){
								kchn="10_阅读";
							}else if(k.equals("education")){
								kchn="11_学习教育";
							}else if(k.equals("audio")){
								kchn="12_音频";
							}else if(k.equals("video")){
								kchn="13_视频";
							}else if(k.equals("image")){
								kchn="14_影音图像";
							}else if(k.equals("appstore")){
								kchn="15_应用商店";
							}else if(k.equals("search")){
								kchn="16_搜索";
							}else if(k.equals("browser")){
								kchn="17_浏览器";
							}else if(k.equals("others")){
								kchn="18_其他";
							}else if(k.equals("safety")){
								kchn="19_安全防护";
							}else if(k.equals("email")){
								kchn="20_邮箱";
							}else if(k.equals("multimsg")){
								kchn="21_彩信";
							}
							
							tag="webtagpeople";

							key="mfg4_"+cdate+"_hspwebset_"+id+"_"+k;
							pcnt=redisserver.scard(key);
							if(pcnt<0)pcnt=0;
							v=String.valueOf(pcnt); //people_cnt
							key="'"+data_time+"','"+id+"','"+tag+"','"+kchn+"','"+v+"'\n";
							fw.write(key);
							
							tag="webtagflux";
							key="mfg4_"+cdate+"_hspwtagflux_"+id+"_"+k;
							v=redisserver.get(key);
							if(v!=null)dnflow=Double.valueOf(v);
							lnflow=(long)dnflow;
							v=String.valueOf(lnflow);  
							key="'"+data_time+"','"+id+"','"+tag+"','"+kchn+"','"+v+"'\n";
							fw.write(key);
							
							num=num+1;
						}
					}
					fw.close();
					logger.info(" Complete get hotspot webapp info, get "+num+" records");
					if(num>0)//有数据存在才考虑进行数据库录入
					{
						Class.forName(ResourcesConfig.MYSQL_SERVER_DRIVER);
						conn=DriverManager.getConnection(url);
						stmt =conn.createStatement();
						sql="delete from tb_mofang_hotspot_cust_tag where tag='webtagpeople' or tag='webtagflux'";
						stmt.execute(sql);
						sql="load data local infile '"+filepath+"' replace into table tb_mofang_hotspot_cust_tag fields terminated by ',' enclosed by '\\'' lines terminated by '\\n'";
						stmt.execute(sql);
						conn.close();	
						logger.info(" Set hotspot webapp info into mysql ok");
					}
				}
			}else{
				logger.info(" Can't get redishotspot webapp info keys.");	
			}
		} catch (Exception e) {
			logger.info(" Thread Flush_Redis_DB Pushing hotspot webapp info to Mysql crashes: "+e.getMessage());
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
		tag=null;
		k=null;
		kchn=null;
		v=null;
		pcnt=0;
		dnflow=0.0;
		lnflow=0;
		num=0;
		filepath=null;
		file=null;
		fw=null;
		
		conn=null;
		sql=null;
		stmt=null;
	}
	
	/**
	 * 抓取每15分钟的热点区域人流量，【4Ghttp流量使用量数据，表中无数据】，推送到mysql的dtdb数据库的tb_mofang_heatmap_ref表格中
	 * 表格tb_mofang_heatmap_ref，字段data_time，tac，ci，cnt
	 */
	public static void PersisHeatMapClockInfo()
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

		logger.info(" Start to get heatmap clock info redis-keys");
		try{
			//获取实例
			redisserver=RedisServer.getInstance();					
			cdate=TimeFormatter.getDate2();        								//获取当前日期YYYY-MM-DD
			keys=redisserver.keys("mfg4_"+cdate+"_hmset_*"); 			//获取当前日期YYYY-MM-DD对应所有imsi信息的keys
			num=0;//统计记录
			if(keys==null||keys.size()==0){
				cdate=TimeFormatter.getYestoday2();
				keys=redisserver.keys("mfg4_"+cdate+"_hmset_*");
			}
			if(keys!=null&&keys.size()>0)
			{
				key=keys.last();//从默认的升序排序中，拿到当前统计的最新时刻
				keysplit=key.split("_");
				hour=keysplit[3];
				minute=keysplit[4];
				keys=redisserver.keys(key="mfg4_"+cdate+"_hmset_"+hour+"_"+minute+"_*");
				if(keys==null||keys.size()==0){
					cdate=TimeFormatter.getYestoday2();
					keys=redisserver.keys(key="mfg4_"+cdate+"_hmset_"+hour+"_"+minute+"_*");
				}
				if(keys!=null&&keys.size()>0){
					data_time=TimeFormatter.getNow(); 		//获取当前时间YYYY-MM-DD HH:mm:ss
					filepath=ResourcesConfig.SYN_SERVER_DATAFILE+"tb_mofang_heatmap_ref.txt";
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
						logger.info(" Complete get heatmap clock info, get "+num+" records");
						if(num>0)//有数据存在才考虑进行数据库录入
						{
							Class.forName(ResourcesConfig.MYSQL_SERVER_DRIVER);
							conn=DriverManager.getConnection(url);
							stmt =conn.createStatement();
							sql="delete from tb_mofang_heatmap_ref";
							stmt.execute(sql);
							sql="load data local infile '"+filepath+"' replace into table tb_mofang_heatmap_ref fields terminated by ',' enclosed by '\\'' lines terminated by '\\n'";
							stmt.execute(sql);
							conn.close();	
							logger.info(" Set heatmap clock info into mysql ok");
						}
					}
			    }else{
					logger.info(" Can't get redis heatmap clock info keys.");	
				}
			}else{
				logger.info(" Can't get redis heatmap clock info keys.");	
			}
		} catch (Exception e) {
			logger.info(" Thread Flush_Redis_DB Pushing heatmap clock info to Mysql crashes: "+e.getMessage());
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
				Redis_To_Mysql.PersisHotspotClockInfo();   		//推送每15分钟的热点区域，热点区域标签对应的人流量，4Ghttp流量使用量数据,ok
				Redis_To_Mysql.PersisHotspotImsiSet();      		//推送当天热点区域的imsi数据明细，ok
				Redis_To_Mysql.PersisHotspotWebClockInfo(); //推送当天热点区域对应的上网标签的人数，流量累计值，ok
				Redis_To_Mysql.PersisHeatMapClockInfo();  		//推送每15分钟的热力图人流量信息，ok
				Thread.sleep(1000*60*15);
			} catch (InterruptedException e) {
				logger.info(" Thread Flush_Redis_DB crashes: "+e.getMessage());
			}
		}
	}
}
