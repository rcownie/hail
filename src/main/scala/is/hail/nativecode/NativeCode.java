package is.hail.nativecode;

import java.io.*;
import java.util.*;
import java.net.URL;
import com.sun.jna.*;

class NativeCode {
  static {
    try {
      String libBoot = libToLocalFile("libboot");
      System.err.println("DEBUG: System.load " + libBoot);
      System.load(libBoot);
      String libHail = libToLocalFile("libhail");
      System.err.println("DEBUG: dlopenGlobal " + libHail);
      long handle = dlopenGlobal(libHail);
      System.err.println("DEBUG: done");
    } catch (Exception e) {
      System.err.println("ERROR: NativeCode.init caught exception");
    }
  }
  
  private static String libToLocalFile(String libName) {
    String path = "";
    try {
      File file = File.createTempFile(libName, ".lib");
      ClassLoader loader = NativeCode.class.getClassLoader();
      InputStream s = null;
      String osName = System.getProperty("os.name");
      if (osName.equals("Linux") || osName.equals("linux")) {
        s = loader.getResourceAsStream("linux-x86-64/" + libName + ".so");
      } else {
        s = loader.getResourceAsStream("darwin/" + libName + ".dylib");
      }
      java.nio.file.Files.copy(s, file.getAbsoluteFile().toPath(),
        java.nio.file.StandardCopyOption.REPLACE_EXISTING
      );
      path = file.getAbsoluteFile().toPath().toString();
    } catch (Exception e) {
      System.err.println("ERROR: NativeCode.init caught exception");
      path = libName + "_resource_not_found";
    }
    return path;
  }
  
  private native static long dlopenGlobal(String path);
  
  private native static long dlclose(long handle);

  final static String getIncludeDir() {
    String name = ClassLoader.getSystemResource("include/hail/hail.h").toString();
    int len = name.length();
    if (len >= 12) {
      name = name.substring(0, len-12);
    }
    if (name.substring(0, 5).equals("file:")) {
      name = name.substring(5, name.length());
    }
    return name;
  }

  final static void forceLoad() {
    System.err.println("DEBUG: forceLoad()");
  }
}
