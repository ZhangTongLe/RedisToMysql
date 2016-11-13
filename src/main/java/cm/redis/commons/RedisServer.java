package cm.redis.commons;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.log4j.Logger;

import cm.redis.commons.ResourcesConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.SortingParams;

/**
 * 2016-09-1 Jedis是2.9.0版本，对应的Redis服务器是3.2.3版本，用于构建cluster连接池与操作的封装类
 * @author nicolashsu
 *
 */
public class RedisServer {
	//构建redis连接池对象实例
	private static JedisPool jedisPool;

	//单例模式实现客户端管理类
	private static RedisServer INSTANCE=new RedisServer();

	public static Logger logger=Logger.getLogger(RedisServer.class);
	
	//初始化构造函数
	private RedisServer()
	{
		//构建jedis连接池配置参数
		JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(ResourcesConfig.MAX_ACTIVE);
        config.setMaxIdle(ResourcesConfig.MAX_IDLE);
        config.setMaxWaitMillis(ResourcesConfig.MAX_WAIT);
        config.setTestOnBorrow(ResourcesConfig.TEST_ON_BORROW);
        jedisPool=new JedisPool(config,ResourcesConfig.REDIS_SERVER_IP, ResourcesConfig.REDIS_SERVER_PORT,ResourcesConfig.TIMEOUT);
	}
	
	/**
	 * 获取缓存管理器唯一实例
	 * @return
	 */
	public static RedisServer getInstance() {
		if(INSTANCE==null){
			synchronized (RedisServer.class) {
				if(INSTANCE==null){
					INSTANCE=new RedisServer();
				}
			}
		}
		return INSTANCE;
	}
	
	//关闭连接池，销毁连接池，在web清理时进行调用
	public static void close(){
		 try {
			 if(jedisPool!=null){
				 jedisPool.close();
				 jedisPool=null;
				 INSTANCE=null;
			 }
		} catch (Exception e) {
			logger.error(" Close jedisPool error: ", e);  
		}
	}

	/*通用key操作*/
	/**
	 * 判断key值是否存在
	 * @param key
	 * @return true为存在，false为不存在
	 */
	public boolean exists(String key){
		Jedis jedis=null;
		try{
			jedis=jedisPool.getResource();  //获取jedis连接池
			return jedis.exists(key);
		}catch(Exception ex){
			logger.info("jedis operation error:"+ex.getMessage());
			return false;
		}finally {
			if(jedis!=null)jedis.close();		//使用完毕归还资源
		}
	}
	
	/**
	 * 删除key值
	 * @param key
	 * @return 被删除的键的数目
	 */
	public long del(String key){
		Jedis jedis=null;
		try{
			jedis=jedisPool.getResource();  //获取jedis连接池
			return jedis.del(key);
		}catch(Exception ex){
			logger.info("jedis operation error:"+ex.getMessage());
			return 0;
		}finally {
			if(jedis!=null)jedis.close();		//使用完毕归还资源
		}
	}
	
	/**
	 * 自定义模糊匹配获取所有的keys
	 * @param pattern
	 * @return TreeSet，这个结构有个好处是已经排序，可以直接获取第一个元素，默认升序排序
	 */
	public TreeSet<String> keys(String pattern){
        TreeSet<String> keys = new TreeSet<String>();
        Jedis jedis=null;
        try {  
        	jedis=jedisPool.getResource();  //获取jedis连接池
            keys.addAll(jedis.keys(pattern));
            return keys;
        } catch(Exception ex){  
            logger.info("Getting keys error: "+ex.getMessage());  
            return null;
        } finally{   
        	if(jedis!=null)jedis.close();//归还资源  
       }  
    }
	
	/**
	 * 游标方式获取对应的key，比keys操作更加节省资源，便于使用，方式1，按照需求获取，无需一次获取多个keys，节约后台内存
	 * 但是需要调用程序自行去重复key，自行对key做排序，并且按需继续提供params参数配置
	 * @param cursor 游标，初始为0，最后返回为0，则表示遍历完成
	 * @param params 游标对应的匹配参数包括count和match
	 * @return
	 */
	public ScanResult<String> scan(String cursor, ScanParams params){
		ScanResult<String> scankey=null;
		Jedis jedis=null;
		 try {  
        	jedis=jedisPool.getResource();  //获取jedis连接池
        	scankey=jedis.scan(cursor, params);
            return scankey;
		 } catch(Exception ex){  
            logger.info("Scan keys error: "+ex.getMessage());   
		 } finally{   
        	if(jedis!=null)jedis.close();//归还资源  
		 }
		 return null;
	}
	
	/**
	 * 游标方式获取对应的key，比keys操作更加节省资源，便于使用，一次获取全部keys，比较消耗内存，
	 * 已经去重复，完成排序，仅需提供params参数模式配置
	 * @param cursor 游标，初始为0，最后返回为0，则表示遍历完成
	 * @param params 游标对应的匹配参数包括count和match
	 * @return
	 */
	public TreeSet<String> scan(String pattern){
		TreeSet<String> keys = new TreeSet<String>();
		ScanResult<String> scankey=null;
		List<String> tmplist=null;
		String cursor=null;
		ScanParams params=new ScanParams();
		Jedis jedis=null;
		int i=0;
        try {
        	cursor="0";
        	params.match(pattern);
        	params.count(10000);
        	jedis=jedisPool.getResource();  //获取jedis连接池
        	do{
        		scankey=jedis.scan(cursor, params);
        		if(scankey!=null)
        		{
        			cursor=scankey.getStringCursor();
        			tmplist=scankey.getResult();
        			if(tmplist!=null&&tmplist.size()>0){
        				if(tmplist.get(0).contains("empty list or set")==false)
        				{
        					for(i=0;i<tmplist.size();i++)keys.add(tmplist.get(i));//将String元素逐个加入，默认重复不会加入
        				}
        			}
        		}
        	}while(cursor.equals("0")==false); 
        } catch(Exception ex){  
        	logger.info("Scan keys error: "+ex.getMessage());  
            return null;
        } finally{  
        	if(jedis!=null)jedis.close();//归还资源  
        }  
        return keys;  
	}
	/*通用key操作结束*/
	
	/*String操作*/
	/**
	 * 添加string value值
	 * @param key
	 * @param value
	 */
	public void set(String key, String value){
		Jedis jedis=null;
		try{
			jedis=jedisPool.getResource();  //获取jedis连接池
			jedis.set(key, value);
		}catch(Exception ex){
			logger.info("jedis operation error:"+ex.getMessage());
		}finally {
			if(jedis!=null)jedis.close();		//使用完毕归还资源
		}
	}
	
	/**
	 * 返回string value值
	 * @param key
	 * @return
	 */
	public String get(String key){
		Jedis jedis=null;
		try{
			jedis=jedisPool.getResource();  //获取jedis连接池
			return jedis.get(key);
		}catch(Exception ex){
			logger.info("jedis operation error:"+ex.getMessage());
			return null;
		}finally {
			if(jedis!=null)jedis.close();		//使用完毕归还资源
		}
	}
	
	/**
	 * 对键值进行自增计数，将指定主键key的value值加1，返回新值，key不存在则添加，value设为1
	 * @param key
	 * @return 返回最新的自增值
	 */
	public long incr(String key){
		Jedis jedis=null;
		try{
			jedis=jedisPool.getResource();  //获取jedis连接池
			return jedis.incr(key);
		}catch(Exception ex){
			logger.info("jedis operation error:"+ex.getMessage());
			return 0;
		}finally {
			if(jedis!=null)jedis.close();		//使用完毕归还资源
		}
	}
	/*String操作结束*/
	
	/*list操作封装*/
	/**
	 * 从list左边插入数值，如果key值不存在，会自动创建并添加元素
	 * @param key
	 * @param value
	 */
	public void lpush(String key,String value){
		Jedis jedis=null;
		try{
			jedis=jedisPool.getResource();  //获取jedis连接池
			jedis.lpush(key, value);
		}catch(Exception ex){
			logger.info("jedis operation error:"+ex.getMessage());
		}finally {
			if(jedis!=null)jedis.close();		//使用完毕归还资源
		}
	}
	
	/**
	 * 从list右边插入数据，如果key值不存在，会自动创建并添加元素
	 * @param key
	 * @param value
	 */
	public void rpush(String key,String value){
		Jedis jedis=null;
		try{
			jedis=jedisPool.getResource();  //获取jedis连接池
			jedis.rpush(key, value);
		}catch(Exception ex){
			logger.info("jedis operation error:"+ex.getMessage());
		}finally {
			if(jedis!=null)jedis.close();		//使用完毕归还资源
		}
	}
	
	/**
	 * 从list左边弹出值
	 * @param key
	 * @param value
	 * @return 弹出的值，key不存在返回null
	 */
	public String lpop(String key){
		Jedis jedis=null;
		try{
			jedis=jedisPool.getResource();  //获取jedis连接池
			return jedis.lpop(key);
		}catch(Exception ex){
			logger.info("jedis operation error:"+ex.getMessage());
			return null;
		}finally {
			if(jedis!=null)jedis.close();		//使用完毕归还资源
		}
	}
	
	/**
	 * 从list从右边弹出值
	 * @param key
	 * @param value
	 * @return 弹出的值，key不存在返回null
	 */
	public String rpop(String key){
		Jedis jedis=null;
		try{
			jedis=jedisPool.getResource();  //获取jedis连接池
			return jedis.rpop(key);
		}catch(Exception ex){
			logger.info("jedis operation error:"+ex.getMessage());
			return null;
		}finally {
			if(jedis!=null)jedis.close();		//使用完毕归还资源
		}
	}
	
	/**
	 * 返回获得的区域value list
	 * @param key
	 * @param start 起始位置，从0开始
	 * @param end -1代表数组最末位置，否则表示结束位置，从0开始计算
	 * @return
	 */
	public List<String> lrange(String key,long start, long end){
		Jedis jedis=null;
		try{
			jedis=jedisPool.getResource();  //获取jedis连接池
			return jedis.lrange(key, start, end);
		}catch(Exception ex){
			logger.info("jedis operation error:"+ex.getMessage());
			return null;
		}finally {
			if(jedis!=null)jedis.close();		//使用完毕归还资源
		}
	}
	
	/*list操作封装结束*/
	
	/*set集合操作封装*/
	/**
	 * 添加set元素操作
	 * @param key
	 * @param value
	 */
	public void sadd(String key, String value){
		Jedis jedis=null;
		try{
			jedis=jedisPool.getResource();  //获取jedis连接池
			jedis.sadd(key,value);
		}catch(Exception ex){
			logger.info("jedis operation error:"+ex.getMessage());
		}finally {
			if(jedis!=null)jedis.close();		//使用完毕归还资源
		}
	}
	
	/**
	 * 检查集合元素数量
	 * @param key 集合的key值
	 * @return 集合的元素总数
	 */
	public Long scard(String key){
		Long res=new Long(0);
		Jedis jedis=null;
		try{
			jedis=jedisPool.getResource();  //获取jedis连接池
			return jedis.scard(key);
		}catch(Exception ex){
			logger.info("jedis operation error:"+ex.getMessage());
			return res;
		}finally {
			if(jedis!=null)jedis.close();		//使用完毕归还资源
		}
	}
	
	/**
	 * 获取集合中的全部元素
	 * @param key
	 * @return
	 */
	public Set<String> smembers(String key){
		Jedis jedis=null;
		try{
			jedis=jedisPool.getResource();  //获取jedis连接池
			return jedis.smembers(key);
		}catch(Exception ex){
			logger.info("jedis operation error:"+ex.getMessage());
			return null;
		}finally {
			if(jedis!=null)jedis.close();		//使用完毕归还资源
		}
	}
	
	/**
	 * 检索集合中的元素，相比较smembers，效率更高，不锁表，方式1，按照需求逐步获取，节约内存
	 * 但是需要调用程序自行对元素做排序，并且按需继续提供params参数配置
	 * @param key 集合key
	 * @param cursor 游标，0开始，再次获取0表示结束
	 * @param params 标记需要检索的元素模式
	 * @return
	 */
	public ScanResult<String> sscan(String key, String cursor,ScanParams params){
		Jedis jedis=null;
		try{
			jedis=jedisPool.getResource();  //获取jedis连接池
			return jedis.sscan(key, cursor, params);
		}catch(Exception ex){
			logger.info("jedis operation error:"+ex.getMessage());
			return null;
		}finally {
			if(jedis!=null)jedis.close();		//使用完毕归还资源
		}
	}
	
	/**
	 * 检索集合中的元素，相比较smembers，效率更高，不锁表，方式2，全部获取元素，可能会比较消耗内存
	 * 但是需要调用程序自行对元素做排序，并且按需继续提供params参数配置
	 * @param key 集合key
	 * @param cursor 游标，0开始，再次获取0表示结束
	 * @param params 标记需要检索的元素模式
	 * @return
	 */
	public TreeSet<String> sscan(String key, String pattern){
		TreeSet<String> members = new TreeSet<String>();
		ScanResult<String> scankey=null;
		List<String> tmplist=null;
		String cursor=null;
		ScanParams params=new ScanParams();
		Jedis jedis=null;
		int i=0;
        try {
        	cursor="0";
        	if(pattern!=null)params.match(pattern);
        	params.count(500);
        	jedis=jedisPool.getResource();  //获取jedis连接池
        	do{
        		scankey=jedis.sscan(key, cursor, params);
        		if(scankey!=null)
        		{
        			cursor=scankey.getStringCursor();
        			tmplist=scankey.getResult();
        			if(tmplist!=null&&tmplist.size()>0){
        				if(tmplist.get(0).contains("empty list or set")==false)
        				{
        					for(i=0;i<tmplist.size();i++)members.add(tmplist.get(i));//将String元素逐个加入，默认重复不会加入
        				}
        			}
        		}
        	}while(cursor.equals("0")==false); 
        } catch(Exception ex){  
        	logger.info("Scan keys error: "+ex.getMessage());  
            return null;
        } finally{  
        	if(jedis!=null)jedis.close();//归还资源  
        }  
        return members;
	}
	/*set集合操作封装结束*/
	
	/*hash散列操作封装*/
	/**
	 * 判断对应哈希key，field是否存在
	 * @param key
	 * @param field
	 * @return
	 */
	public boolean hexists(String key, String field)
	{
		Jedis jedis=null;
		try{
			jedis=jedisPool.getResource();  //获取jedis连接池
			return jedis.hexists(key, field);
		}catch(Exception ex){
			logger.info("jedis operation error:"+ex.getMessage());
			return false;
		}finally {
			if(jedis!=null)jedis.close();		//使用完毕归还资源
		}
	}
	
	/**
	 * 将哈希表key中的域field的值设为value，key不存在，
	 * 一个新的哈希表被创建，域field已经存在于哈希表中，旧值将被覆盖
	 * @param key
	 * @param field
	 * @param value
	 */
	public void hset(String key, String field, String value)
	{
		Jedis jedis=null;
		try{
			jedis=jedisPool.getResource();  //获取jedis连接池
			jedis.hset(key, field, value);
		}catch(Exception ex){
			logger.info("jedis operation error:"+ex.getMessage());
		}finally {
			if(jedis!=null)jedis.close();		//使用完毕归还资源
		}
	}
	
	/**
	 * 设置哈希表中的字段及对应的值
	 * @param key
	 * @param field_value,哈希键值数组
	 */
	public void hmset(String key, Map<String, String> field_value)
	{
		Jedis jedis=null;
		try{
			jedis=jedisPool.getResource();  //获取jedis连接池
			jedis.hmset(key, field_value);
		}catch(Exception ex){
			logger.info("jedis operation error:"+ex.getMessage());
		}finally {
			if(jedis!=null)jedis.close();		//使用完毕归还资源
		}
	}
	
	/**
	 * 获取哈希表中域的值
	 * @param key
	 * @param field
	 * @return 如果key或者field不存在，结果返回为null
	 */
	public String hget(String key, String field)
	{
		Jedis jedis=null;
		try{
			jedis=jedisPool.getResource();  //获取jedis连接池
			return jedis.hget(key, field);
		}catch(Exception ex){
			logger.info("jedis operation error:"+ex.getMessage());
			return null;
		}finally {
			if(jedis!=null)jedis.close();		//使用完毕归还资源
		}
	}
	
	/**
	 * 获取hash key对应的所有feild和value对
	 * @param key
	 * @return
	 */
	public Map<String, String> hgetall(String key)
	{
		Jedis jedis=null;
		try{
			jedis=jedisPool.getResource();  //获取jedis连接池
			return jedis.hgetAll(key);
		}catch(Exception ex){
			logger.info("jedis operation error:"+ex.getMessage());
			return null;
		}finally {
			if(jedis!=null)jedis.close();		//使用完毕归还资源
		}
	}
	
	/**
	 * 返回hash key中对应的全部fields名称
	 * @param key
	 * @return
	 */
	public Set<String> hkeys(String key){
		Jedis jedis=null;
		try{
			jedis=jedisPool.getResource();
			return jedis.hkeys(key);
		}catch(Exception ex){
			logger.info("jedis operation error:"+ex.getMessage());
			return null;
		}finally {
			if(jedis!=null)jedis.close();
		}
	}
	/*hash散列操作封装结束*/
	
	/*排序操作封装*/
	/**
	 * 对redis中的list，set，order set对应的key值进行默认升序排序
	 * @param key
	 * @return list, set, order set中的排序结果
	 */
	public List<String> redis_sort1(String key)
	{
		Jedis jedis=null;
		try{
			jedis=jedisPool.getResource();  //获取jedis连接池
			return jedis.sort(key);
		}catch(Exception ex){
			logger.info("jedis operation error:"+ex.getMessage());
			return null;
		}finally {
			if(jedis!=null)jedis.close();		//使用完毕归还资源
		}
	}
	
	/**
	 * 对redis中的list，set，order set对应的key值进行排序，jedis支持by 与 get
	 * @param key
	 * @param sortingParameters
	 * @return list, set, order set中的排序结果
	 */
	public List<String> redis_sort2(String key, SortingParams sortingParameters)
	{
		Jedis jedis=null;
		try{
			jedis=jedisPool.getResource();  //获取jedis连接池
			return jedis.sort(key, sortingParameters);
		}catch(Exception ex){
			logger.info("jedis operation error:"+ex.getMessage());
			return null;
		}finally {
			if(jedis!=null)jedis.close();		//使用完毕归还资源
		}
	}
	/*排序操作封装结束*/
	
}


