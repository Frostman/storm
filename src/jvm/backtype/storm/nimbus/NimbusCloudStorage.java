package backtype.storm.nimbus;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.utils.Utils;

import com.netflix.curator.framework.CuratorFramework;

/**
 * NimbusCloudStorage is an implementation of INimbusStorage which gives out a solution of nimbus-single-node problem.
 * Using NimbusCloudStorage you can now start nimbus processes as many as you want on your clusters just ensure that one machine has only one nimbus process. 
 * 
 * usage: add [nimbus.storage: "backtype.storm.nimbus.NimbusCloudStorage"] to storm/conf/storm.yaml
 * 
 * internal keynotes:
 * NimbusCloudStorage will start a loop thread on init when "storm nimbus" command is executed. 
 * The loop thread will then recurrently compare the zk-assignments and local dir "nimbus/stormdist" to download codes.
 * The loop interval in seconds (default 2) can be customerized by add [nimbus.storage.cloud.loop.secs:"10"] to storm/conf/storm.yaml
 * 
 * @author yveschina
 *
 */
public class NimbusCloudStorage implements INimbusStorage {

	private static Logger logger = Logger.getLogger(NimbusCloudStorage.class);

	private int nimbus_cloud_storage_loop_secs = 2;
	private String storm_nimbus_dir;
	@SuppressWarnings("rawtypes")
	private Map conf;

	private static volatile Thread storageSyncThread = null;
	private static final String storm_assignments_folder = "assignments";
	private static final String storm_stormdist_folder = "stormdist";
	private static final String storm_nimbus_tmp_folder = "tmp";
	public static final String STORM_NIMBUS_CLOUD_STORAGE_LOOP_SECS = "nimbus.storage.cloud.loop.secs";

	@Override
	public void init(@SuppressWarnings("rawtypes") Map conf) {
		this.conf = conf;
		setNimbusLocalDir();
		setLoopSecs();
		try {
			FileUtils.forceMkdir(new File(storm_nimbus_dir));
		} catch (IOException e) {
			e.printStackTrace();
		}
		initStorageSyncThread();
	}

	private void setNimbusLocalDir() {
		String storm_local_dir = (String) conf.get(Config.STORM_LOCAL_DIR);
		String nimbus_local_dir = (String) conf.get("nimbus.local.dir");
		String local_dir = storm_local_dir;
		if (StringUtils.isNotBlank(nimbus_local_dir)) {
			local_dir = nimbus_local_dir;
		}
		storm_nimbus_dir = local_dir + "/nimbus";
	}

	private void setLoopSecs() {
		if (conf.containsKey(STORM_NIMBUS_CLOUD_STORAGE_LOOP_SECS)) {
			nimbus_cloud_storage_loop_secs = Integer.valueOf(conf.get(STORM_NIMBUS_CLOUD_STORAGE_LOOP_SECS).toString()).intValue();
		}
	}

	private void initStorageSyncThread() {
		if (storageSyncThread == null) {
			storageSyncThread = new Thread(new Runnable() {
				@SuppressWarnings("unchecked")
				@Override
				public void run() {
					logger.info("storage sync thread start running");
					// init curatorFramework
					List<String> servers = (List<String>) conf.get(Config.STORM_ZOOKEEPER_SERVERS);
					Object port = conf.get(Config.STORM_ZOOKEEPER_PORT);
					CuratorFramework curatorFramework = Utils.newCurator(conf, servers, port);
					curatorFramework.start();

					// Get /storm/assignments Path
					String storm_zookeeper_root = (String) conf.get(Config.STORM_ZOOKEEPER_ROOT);
					String assignments_dir = storm_zookeeper_root + "/" + storm_assignments_folder;

					String storm_stormdist_dir = storm_nimbus_dir + "/" + storm_stormdist_folder;
					File storm_stormdist = new File(storm_stormdist_dir);
					if (!storm_stormdist.exists()) {
						try {
							FileUtils.forceMkdir(storm_stormdist);
						} catch (IOException e) {
							e.printStackTrace();
						}
					}

					do {
						// check to download storm code
						try {
							Thread.sleep(nimbus_cloud_storage_loop_secs*1000);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}

						// get assignments storm-id from zk
						List<String> zkStorms = null;
						try {
							zkStorms = curatorFramework.getChildren().forPath(assignments_dir);
						} catch (Exception e1) {
							e1.printStackTrace();
						}

						if (null == zkStorms || zkStorms.size() == 0) {
							continue;
						}

						// get downloaded storm-id from local
						List<String> localStorms = list(storm_stormdist_folder);
						if (null == localStorms) {
							localStorms = new ArrayList<String>();
						}

						// find storms need to download code
						Set<String> localStormsSet = new HashSet<String>(localStorms);
						List<String> stormsAbsent = new ArrayList<String>();
						for (String zkStorm : zkStorms) {
							if (!localStormsSet.contains(zkStorm)) {
								stormsAbsent.add(zkStorm);
							}
						}

						if (stormsAbsent.size() == 0) {
							continue;
						}

						// download code from leader
						for (String storm : stormsAbsent) {
							logger.info("start to download storm code!storm-id:" + storm);
							try {
								byte[] data = curatorFramework.getData().forPath(assignments_dir + "/" + storm);
								Object assignment = (Object) Utils.deserialize(data);
								String master_code_dir = assignment.getClass().getDeclaredField("master_code_dir").get(assignment).toString();

								String storm_dir_tmp = storm_nimbus_dir + "/" + storm_nimbus_tmp_folder + "/" + UUID.randomUUID();
								FileUtils.forceMkdir(new File(storm_dir_tmp));

								Utils.downloadFromMaster(conf, master_code_dir + "/stormjar.jar", storm_dir_tmp + "/stormjar.jar");
								Utils.downloadFromMaster(conf, master_code_dir + "/stormcode.ser", storm_dir_tmp + "/stormcode.ser");
								Utils.downloadFromMaster(conf, master_code_dir + "/stormconf.ser", storm_dir_tmp + "/stormconf.ser");

								String storm_dir_stormdist = storm_nimbus_dir + "/" + storm_stormdist_folder + "/" + storm;
								FileUtils.moveDirectory(new File(storm_dir_tmp), new File(storm_dir_stormdist));
							} catch (Exception e) {
								e.printStackTrace();
							}
							logger.info("storm code download complete!storm-id:" + storm);
						}
					} while (true);
				}
			});
			storageSyncThread.start();
		}
	}

	@Override
	public InputStream open(String path) {
		FileInputStream inputStream = null;
		try {
			inputStream = new FileInputStream(getAbsolutePath(path));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		return inputStream;
	}

	@Override
	public OutputStream create(String path) {
		FileOutputStream outputStream = null;
		try {
			outputStream = new FileOutputStream(getAbsolutePath(path));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		return outputStream;
	}

	@Override
	public List<String> list(String path) {
		File file = new File(getAbsolutePath(path));
		String[] files = file.list();
		return files == null ? null : Arrays.asList(files);
	}

	@Override
	public void delete(String path) {
		File file = new File(getAbsolutePath(path));
		if (file.exists()) {
			try {
				FileUtils.forceDelete(file);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void mkdirs(String path) {
		try {
			FileUtils.forceMkdir(new File(getAbsolutePath(path)));
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	@Override
	public boolean isSupportDistributed() {
		return true;
	}

	private String getAbsolutePath(String path) {
		String absolute_path = storm_nimbus_dir;
		if (path.startsWith("/")) {
			absolute_path += path;
		} else {
			absolute_path += "/" + path;
		}
		return absolute_path;
	}
}
