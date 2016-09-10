package cm.redis.settomysql;

import java.io.File;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Iterator;
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
	 * 主函数
	 * @param args
	 */
	public static void main(String[] args)
	{
		//将hotspot，业务关注的热点区域的人流量，流量数据信息，推送到mysql中
		//表格是tb_mofang_hotspot_flow_today，字段data_time，id，day，hour，minute，people_cnt，net_flow
		RedisServer redisserver=null;
		TreeSet<String> keys=null; 
		Iterator<String> keylist =null;
		String[] keysplit=null;
		String cdate=null;
		int num=0;
		
		String key=null;
		
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
		String filepath=null;
		FileWriter fw=null;	
		
		Connection conn=null;
		String sql=null;
		String url=ResourcesConfig.MYSQL_SERVER_URL+"?user="+ ResourcesConfig.MYSQL_USER
				+"&password="+ResourcesConfig.MYSQL_PASSWD+"&characterEncoding=UTF8";
		Statement stmt=null;
		try {
			while(true)
			{
				logger.info(" Start to get hotspot redis-keys");
				//获取实例
				redisserver=RedisServer.getInstance();		
				data_time=TimeFormatter.getNow(); 	//获取当前时间YYYY-MM-DD HH:mm:ss
				day=TimeFormatter.getDate(); 			//当天日期 YYYYMMDD，删除mysql对应的当天记录		
				cdate=data_time.substring(0,10);        //获取当前日期YYYY-MM-DD
				keys=redisserver.keys("mfg4_"+cdate+"_hspset_*"); 				//获取所有的hotspot对应的imsi集合
				num=0;
				
				conn=null;
				
				filepath=ResourcesConfig.SYN_SERVER_DATAFILE+"tb_mofang_hotspot_flow_today.txt";
				File file = new File(filepath);
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
			    }	
				logger.info(" Complete get hotspot redis-keys, get "+num+" records");	
				
				
				Class.forName(ResourcesConfig.MYSQL_SERVER_DRIVER);
				conn=DriverManager.getConnection(url);
				stmt =conn.createStatement();
				sql="delete from tb_mofang_hotspot_flow_today where day='"+day+"'";
				stmt.execute(sql);
				sql="load data local infile '"+filepath+"' replace into table tb_mofang_hotspot_flow_today fields terminated by ',' enclosed by '\\'' lines terminated by '\\n'";
				stmt.execute(sql);
				conn.close();	
				logger.info(" Set into mysql ok");

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
				fw=null;
				
				conn=null;
				sql=null;
				
				Thread.sleep(1000*60*5);
			}
		} catch (Exception e) {
			logger.info(" Thread Flush_Redis_DB crashes: "+e.getMessage());
		}
	}
}
