package com.nec;


import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.*;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NECVEPlugin implements DevicePlugin, DevicePluginScheduler {
  public static final Logger LOG = LoggerFactory.getLogger(NECVEPlugin.class);

  private static final String[] DEFAULT_BINARY_SEARCH_DIRS = new String[]{
      "/usr/bin", "/bin", "/opt/nec/ve/bin"};

  private String binaryName = "veGet.py";
  private File binaryFile;

  public NECVEPlugin() throws Exception {
    String binaryName = System.getenv("NEC_VE_GET_SCRIPT_NAME");
    if (null != binaryName) {
      LOG.info("Use {} as script name.", binaryName);
      this.binaryName = binaryName;
    }
    // search binary exists
    boolean found = false;
    for (String dir : DEFAULT_BINARY_SEARCH_DIRS) {
      binaryFile = new File(dir, binaryName);
      if (binaryFile.exists()) {
        found = true;
        this.binaryName = binaryFile.getAbsolutePath();
        break;
      }
    }
    if (!found) {
      LOG.error("No \"{}\" found in below path \"{}\"",
          DEFAULT_BINARY_SEARCH_DIRS);
      throw new Exception("No binary found for " + NECVEPlugin.class);
    }

  }

  public DeviceRegisterRequest getRegisterRequestInfo() {
    return DeviceRegisterRequest.Builder.newInstance()
        .setResourceName("nec.com/ve").build();
  }

  public Set<Device> getDevices() {
    TreeSet<Device> r = new TreeSet<Device>();

    String output = null;
    Shell.ShellCommandExecutor shexec = new Shell.ShellCommandExecutor(
        new String[]{binaryName});
    try {
      shexec.execute();
      output = shexec.getOutput();
      parseOutput(r,output);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return r;
//    r.add(Device.Builder.newInstance().setID(0).setDevPath("/dev/ve0")
//        .setMajorNumber(243)
//        .setMinorNumber(0)
//        .setBusID("0000:65:00.0")
//        .setHealthy(true).build());
//    r.add(Device.Builder.newInstance().setID(1).setDevPath("/dev/ve1")
//        .setMajorNumber(243)
//        .setMinorNumber(1)
//        .setBusID("0000:65:01.0")
//        .setHealthy(true).build());
//    r.add(Device.Builder.newInstance().setID(2).setDevPath("/dev/ve2")
//        .setMajorNumber(243)
//        .setMinorNumber(2)
//        .setBusID("0000:65:02.0")
//        .setHealthy(true).build());
  }
  /**
   * Sample one line in output:
   * id=0, dev=/dev/ve0, state=ONLINE, busId=0000:65:00.0, major=243, minor=0
   */
  private void parseOutput(TreeSet<Device> r, String output) {
    String[] lines = output.split("\n");
    for (String line : lines) {
      String[] keyvalues = line.trim().split(",");
      for (String keyvalue : keyvalues) {
        String[] tokens = keyvalue.trim().split("=");
        Device.Builder builder = Device.Builder.newInstance();
        if (tokens.length != 2) {
          LOG.error("Unknown format of script output! Skip this line");
          break;
        }
        if (tokens[0].equals("id")) {
          builder.setID(Integer.valueOf(tokens[1]));
        }
        if (tokens[0].equals("dev")) {
          builder.setDevPath(tokens[1]);
        }
        if (tokens[0].equals("state") &&
            tokens[1].equals("ONLINE")) {
          builder.setHealthy(true);
        }
        if (tokens[0].equals("busId")) {
          builder.setBusID(tokens[1]);
        }
        if (tokens[0].equals("major")) {
          builder.setMajorNumber(Integer.valueOf(tokens[1]));
        }
        if (tokens[0].equals("minor")) {
          builder.setMinorNumber(Integer.valueOf(tokens[1]));
        }
        r.add(builder.build());
      }// for key value pars
    }
  }

  public DeviceRuntimeSpec onDeviceUse(Set<Device> allocatedDevices, String runtime) {
    return null;
  }

  public void onDevicesReleased(Set<Device> releasedDevices) {

  }

  public Set<Device> allocateDevices(Set<Device> availableDevices, Integer count) {
    // Can consider topology, utilization.etc
    Set<Device> allocated = new TreeSet<Device>();
    int number = 0;
    for (Device d : availableDevices) {
      allocated.add(d);
      number++;
      if (number == count) {
        break;
      }
    }
    return allocated;
  }
}
