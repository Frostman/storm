package backtype.storm.task;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.utils.Utils;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.utils.ZKPaths;

/**
 * CustomerContext is used by spoult/bolt to save customerized data which could
 * be shown in storm-ui for task dimension monitor requirments.
 * 
 * @author yveschina
 * 
 */
public class CustomerContext {

	private static Logger logger = Logger.getLogger(CustomerContext.class);
	/**
	 * 
	 * Map<stormid,Map<taskid,Map<k,v>>
	 * 
	 */
	private static final Map<String, ConcurrentHashMap<Integer, Map<String, Object>>> workerContextMap = new ConcurrentHashMap<String, ConcurrentHashMap<Integer, Map<String, Object>>>();
	public static final String storm_customercontext_folder = "customercontext";
	private static Thread monitorThread = null;

	public static void start(@SuppressWarnings("rawtypes") final Map conf) {
		synchronized (CustomerContext.class) {
			if (monitorThread == null) {
				monitorThread = new Thread(new Runnable() {

					@SuppressWarnings("unchecked")
					@Override
					public void run() {

						logger.info("CustomerContext serialization thread start running");
						
						List<String> servers = (List<String>) conf.get(Config.STORM_ZOOKEEPER_SERVERS);
						Object port = conf.get(Config.STORM_ZOOKEEPER_PORT);
						CuratorFramework curatorFramework = Utils.newCurator(conf, servers, port);
						curatorFramework.start();

						do {
							
							if (workerContextMap.size() == 0) {
								continue;
							}

							for (Entry<String, ConcurrentHashMap<Integer, Map<String, Object>>> workerContextEntry : workerContextMap.entrySet()) {
								String stormId = workerContextEntry.getKey();
								ConcurrentHashMap<Integer, Map<String, Object>> stormContextMap = workerContextEntry.getValue();
								for (Entry<Integer, Map<String, Object>> stormContextEntry : stormContextMap.entrySet()) {
									Integer taskId = stormContextEntry.getKey();
									Map<String, Object> taskContextMap = stormContextEntry.getValue();
									String path = (String) conf.get(Config.STORM_ZOOKEEPER_ROOT) + "/" + storm_customercontext_folder + "/" + stormId
											+ "/" + taskId;
									try {
										ZKPaths.mkdirs(curatorFramework.getZookeeperClient().getZooKeeper(), path);
										curatorFramework.setData().forPath(path, Utils.serialize(taskContextMap));
									} catch (Exception e) {
										e.printStackTrace();
									}
								}
							}
							
							try {
								Thread.sleep(2000);
							} catch (InterruptedException e) {
								e.printStackTrace();
							}
						} while (true);
					}
				});
				monitorThread.start();
			}
		}
	}

	/**
	 * save customer context k,v
	 * 
	 * @param key
	 * @param value
	 * @param topologyContext
	 */
	public static void put(String key, Object value, TopologyContext topologyContext) {
		String stormId = topologyContext.getStormId();
		ConcurrentHashMap<Integer, Map<String, Object>> stormContextMap = null;
		if (workerContextMap.containsKey(stormId)) {
			stormContextMap = workerContextMap.get(stormId);
		} else {
			stormContextMap = new ConcurrentHashMap<Integer, Map<String, Object>>();
			workerContextMap.put(stormId, stormContextMap);
		}
		Integer taskId = topologyContext.getThisTaskId();
		Map<String, Object> taskContextMap = null;
		if (stormContextMap.containsKey(taskId)) {
			taskContextMap = stormContextMap.get(taskId);
		} else {
			taskContextMap = new HashMap<String, Object>();
			stormContextMap.put(taskId, taskContextMap);
		}
		taskContextMap.put(key, value);
	}

	/**
	 * get value by k saved by CustomerContext.put(String key, Object value,
	 * TopologyContext topologyContext)
	 * 
	 * @param key
	 * @param topologyContext
	 * @return Object
	 */
	public static Object get(String key, TopologyContext topologyContext) {
		Object object = null;
		String stormId = topologyContext.getStormId();
		if (workerContextMap.containsKey(stormId)) {
			ConcurrentHashMap<Integer, Map<String, Object>> stormContextMap = workerContextMap.get(stormId);
			Integer taskId = topologyContext.getThisTaskId();
			Map<String, Object> taskContextMap = stormContextMap.get(taskId);
			if (taskContextMap.containsKey(key)) {
				object = taskContextMap.get(key);
			}
		}
		return object;
	}
}
