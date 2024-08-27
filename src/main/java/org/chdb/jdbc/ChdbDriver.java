package org.chdb.jdbc;

import java.sql.*;
import java.util.*;
import java.util.logging.*;

/**
 * chdb JDBC driver with FFM
 *
 * @author linux_china
 */
public class ChdbDriver implements Driver, DriverAction {
    private static Driver registeredDriver;

    static {
        try {
            register();
        } catch (SQLException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        return new ChdbConnection(url, info);
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url.startsWith("jdbc:chdb::");
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return 24;
    }

    @Override
    public int getMinorVersion() {
        return 5;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }

    @Override
    public void deregister() {

    }

    /**
     * Register the driver against {@link DriverManager}. This is done automatically when the class is
     * loaded. Dropping the driver from DriverManager's list is possible using {@link #deregister()}
     * method.
     *
     * @throws IllegalStateException if the driver is already registered
     * @throws SQLException          if registering the driver fails
     */
    public static void register() throws SQLException {
        if (registeredDriver != null) {
            throw new IllegalStateException("Driver is already registered. It can only be registered once.");
        }
        Driver registeredDriver = new ChdbDriver();
        DriverManager.registerDriver(registeredDriver);
        ChdbDriver.registeredDriver = registeredDriver;
    }

}
