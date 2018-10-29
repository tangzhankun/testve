package com.nec;

import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.*;

import java.util.Set;
import java.util.TreeSet;

public class NECVEPlugin implements DevicePlugin, DevicePluginScheduler {
  public DeviceRegisterRequest register() {
    return DeviceRegisterRequest.Builder.newInstance()
        .setResourceName("nec.com/ve").build();
  }

  public Set<Device> getDevices() {
    // cat /sys/class/ve/ve0/device/uevent to get pci bus id
    // use script or
    TreeSet<Device> r = new TreeSet<Device>();
    r.add(Device.Builder.newInstance().setID(0).setDevPath("/dev/ve0")
        .setMajorNumber(243)
        .setMinorNumber(0)
        .setBusID("0000:65:00.0")
        .setHealthy(true).build());
    r.add(Device.Builder.newInstance().setID(1).setDevPath("/dev/ve1")
        .setMajorNumber(243)
        .setMinorNumber(1)
        .setBusID("0000:65:01.0")
        .setHealthy(true).build());
    r.add(Device.Builder.newInstance().setID(2).setDevPath("/dev/ve2")
        .setMajorNumber(243)
        .setMinorNumber(2)
        .setBusID("0000:65:02.0")
        .setHealthy(true).build());
    return r;
  }

  public DeviceRuntimeSpec onDevicesUse(Set<Device> set, String s) {
    return null;
  }

  public void onDevicesReleased(Set<Device> set) {

  }

  public Set<Device> allocateDevices(Set<Device> availableDevices, Integer count) {
    // topology
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
