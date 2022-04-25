package com.agmtopy;

import org.apache.jmeter.DynamicClassLoader;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.SQLException;
import java.util.Arrays;

public class Test {
    public static void main(String[] args) throws SQLException, IOException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
//        System.out.println(111);
//        System.out.println(System.getProperty("java.version"));
//        System.out.println(System.getProperty("java.specification.version"));
//        DataSource dataSource = ShardingJDBCDataSourceFactory.newInstance(ShardingConfigType.FULLROUTING_SHARDING_SHARDINGJDBC_CONFIG);
//        System.out.println("dataSource:" + dataSource);

        String path = "C:/Users/agmtopy/.m2/repository/shardingsphere-benchmark/shardingsphere-benchmark/2.1-SNAPSHOT/shardingsphere-benchmark-2.1-SNAPSHOT.jar";
        File file = new File(path);
        URL[] urls = {file.toURI().toURL()};
        DynamicClassLoader dynamicClassLoader = new DynamicClassLoader(urls);

        loadJar(path);
        ClassLoader.getSystemClassLoader().loadClass("org.apache.shardingsphere.benchmark.db.shardingjdbc.ShardingJDBCDataSourceFactory");

        ClassLoader.getSystemClassLoader().loadClass("org.apache.shardingsphere.benchmark.db.shardingjdbc.ShardingJDBCDataSourceFactory");

        Class<?> aClass = dynamicClassLoader.loadClass("org.apache.shardingsphere.benchmark.db.shardingjdbc.ShardingJDBCDataSourceFactory");
        Object instance = aClass.getDeclaredConstructor().newInstance();
        Method[] methods = aClass.getDeclaredMethods();

        Method main = Arrays.asList(methods).stream().filter(item -> item.getName().equals("main")).findFirst().get();

        main.invoke(instance);
    }


    public static void loadJar(String jarPath) {
        System.setProperty("illegal-access", "permit");
        File jarFile = new File(jarPath);
        // 从URLClassLoader类中获取类所在文件夹的方法，jar也可以认为是一个文件夹
        Method method = null;
        try {
            method = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
        } catch (NoSuchMethodException | SecurityException e1) {
            e1.printStackTrace();
        }
        //获取方法的访问权限以便写回
        boolean accessible = method.isAccessible();
        try {
            method.setAccessible(true);
            // 获取系统类加载器
            URLClassLoader classLoader = (URLClassLoader) ClassLoader.getSystemClassLoader();
            URL url = jarFile.toURI().toURL();
            method.invoke(classLoader, url);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            method.setAccessible(accessible);
        }

    }
}
