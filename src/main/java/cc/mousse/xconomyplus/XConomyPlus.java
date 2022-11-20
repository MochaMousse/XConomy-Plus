package cc.mousse.xconomyplus;

import lombok.val;
import me.yic.xconomy.api.XConomyAPI;
import org.apache.commons.lang3.StringUtils;
import org.bukkit.Bukkit;
import org.bukkit.plugin.java.JavaPlugin;

import java.math.BigDecimal;
import java.sql.*;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author PhineasZ
 */
public final class XConomyPlus extends JavaPlugin {

  private static final String PLUGIN_NAME = "XConomy-Plus";
  private static final String TABLE_NAME = "notice";

  private boolean runningFlag;
  private XConomyAPI xConomyApi;
  private Connection conn;
  private PreparedStatement cntPs;
  private PreparedStatement getPs;
  private PreparedStatement delPs;

  /** 自定义线程池 */
  private static final ThreadPoolExecutor THREAD_POOL =
      new ThreadPoolExecutor(
          1,
          1,
          0L,
          TimeUnit.SECONDS,
          new LinkedBlockingQueue<>(4),
          r -> {
            val thread = new Thread(r);
            thread.setName(PLUGIN_NAME);
            return thread;
          });

  @Override
  public void onEnable() {
    // Plugin startup logic
    this.runningFlag = true;
    // 初始化SQLManager
    getConn();
    if (cntPs == null || getPs == null || delPs == null) {
      this.getLogger().severe("数据库配置错误");
      Bukkit.getPluginManager().disablePlugin(this);
      return;
    }
    xConomyApi = new XConomyAPI();
    THREAD_POOL.execute(
        () -> {
          try {
            execute();
          } catch (Exception e) {
            if (runningFlag) {
              runningFlag = false;
              this.getLogger().severe(e.getMessage());
              Bukkit.getPluginManager().disablePlugin(this);
            }
          }
        });
    this.getLogger().info("插件已加载");
  }

  @Override
  public void onDisable() {
    // Plugin shutdown logic
    this.runningFlag = false;
    closeConn();
    this.getLogger().info("插件已卸载");
  }

  private void execute() throws Exception {
    while (conn != null) {
      // 查询记录数量
      val cntRs = cntPs.executeQuery();
      if (!cntRs.next()) {
        continue;
      }
      val cnt = cntRs.getInt("cnt");
      if (cnt != 0) {
        val getRs = getPs.executeQuery();
        while (getRs.next()) {
          val newBalance = getRs.getBigDecimal("amount");
          val uuid = UUID.fromString(getRs.getString("uuid"));
          val playerData = xConomyApi.getPlayerData(uuid);
          val oldBalance = playerData.getBalance();
          val name = playerData.getName();
          var status = -1;
          var message = "";
          if (newBalance.compareTo(oldBalance) > 0) {
            // 新余额比旧余额多，增加
            status =
                xConomyApi.changePlayerBalance(
                    uuid, name, newBalance.subtract(oldBalance), true, PLUGIN_NAME);
            message =
                "玩家"
                    + name
                    + "["
                    + uuid
                    + "]["
                    + oldBalance
                    + "]增加["
                    + newBalance.subtract(oldBalance)
                    + "]咕咕币，当前余额为["
                    + newBalance
                    + "]咕咕币";
          } else if (newBalance.compareTo(oldBalance) < 0) {
            // 新余额比旧余额多，减少
            if (newBalance.compareTo(new BigDecimal(0)) < 0) {
              // 余额不足时直接设置
              status = xConomyApi.changePlayerBalance(uuid, name, newBalance, null, PLUGIN_NAME);
            } else {
              status =
                  xConomyApi.changePlayerBalance(
                      uuid, name, oldBalance.subtract(newBalance), false, PLUGIN_NAME);
            }
            message =
                "玩家"
                    + name
                    + "["
                    + uuid
                    + "]["
                    + oldBalance
                    + "]减少["
                    + oldBalance.subtract(newBalance)
                    + "]咕咕币，当前余额为["
                    + newBalance
                    + "]咕咕币";
          }
          var course = "玩家" + name + "[" + uuid + "][" + oldBalance + "]";
          switch (status) {
            case 0 -> this.getLogger().info(message);
            case 2 -> this.getLogger().warning(course + "余额不足");
            case 3 -> this.getLogger().warning(course + "余额超出最大值");
            default -> this.getLogger().info(course + "余额相同");
          }
          // 添加到批处理
          delPs.setLong(1, getRs.getLong("id"));
          delPs.addBatch();
        }
        // 执行批处理
        delPs.executeBatch();
        // 提交
        conn.commit();
        delPs.clearBatch();
      } else {
        // 提交
        conn.commit();
      }
    }
  }

  private void getConn() {
    // 加载配置文件
    this.saveDefaultConfig();
    this.reloadConfig();
    // 读取配置文件
    val config = this.getConfig();
    val url = config.getString("datasource.mysql.url");
    val driver = config.getString("datasource.mysql.driver");
    val username = config.getString("datasource.mysql.username");
    val password = config.getString("datasource.mysql.password");
    // 校验配置
    if (StringUtils.isBlank(url)
        || StringUtils.isBlank(driver)
        || StringUtils.isBlank(username)
        || StringUtils.isBlank(password)) {
      // 数据为空，关闭插件
      this.getLogger().severe("数据库配置文件不完整");
      Bukkit.getPluginManager().disablePlugin(this);
      return;
    }
    try {
      // 注册JDBC驱动
      Class.forName(driver);
      conn = DriverManager.getConnection(url, username, password);
      // 设置为手动提交
      conn.setAutoCommit(false);
      cntPs = conn.prepareStatement("SELECT COUNT(1) AS cnt FROM " + TABLE_NAME);
      getPs = conn.prepareStatement("SELECT * FROM " + TABLE_NAME);
      delPs = conn.prepareStatement("DELETE FROM " + TABLE_NAME + " WHERE id = ?");
    } catch (Exception e) {
      this.getLogger().severe(e.getMessage());
      closeConn();
    }
  }

  private void closeConn() {
    if (cntPs != null) {
      try {
        cntPs.close();
      } catch (SQLException e) {
        this.getLogger().severe(e.getMessage());
      }
    }
    if (getPs != null) {
      try {
        getPs.close();
      } catch (SQLException e) {
        this.getLogger().severe(e.getMessage());
      }
    }
    if (delPs != null) {
      try {
        delPs.close();
      } catch (SQLException e) {
        this.getLogger().severe(e.getMessage());
      }
    }
    if (conn != null) {
      try {
        conn.close();
      } catch (SQLException e) {
        this.getLogger().severe(e.getMessage());
      }
    }
  }
}
