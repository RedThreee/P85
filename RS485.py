import serial
import time
import struct
import binascii
import sys
import mysql.connector
from mysql.connector import Error
from datetime import datetime

# 数据库配置（根据您的设置修改）
DB_CONFIG = {
    'host': 'localhost',      # MySQL服务器地址
    'database': 'SZX_DB',  # 数据库名称
    'user': 'root',           # 用户名
    'password': '574776436MySQL',     # 密码
    'port': 3306              # 端口
}

# CRC16 MODBUS 校验计算
def crc16_modbus(data: bytes) -> int:
    """
    计算MODBUS RTU协议的CRC16校验值
    参数: data - 待计算的数据字节
    返回: CRC16值 (整数)
    """
    crc = 0xFFFF
    for b in data:
        crc ^= b
        for _ in range(8):
            if crc & 0x0001:
                crc = (crc >> 1) ^ 0xA001
            else:
                crc >>= 1
    return crc

# 初始化数据库连接
def create_db_connection():
    """
    创建MySQL数据库连接
    返回: 连接对象或None
    """
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        if connection.is_connected():
            db_info = connection.get_server_info()
            print(f"成功连接MySQL服务器! 版本: {db_info}")
            return connection
    except Error as e:
        print(f"数据库连接错误: {e}")
    return None

# 创建数据库表（如果不存在）
def initialize_database(conn):
    """
    初始化数据库表结构
    """
    create_table_query = """
    CREATE TABLE IF NOT EXISTS sensor_data (
        id INT AUTO_INCREMENT PRIMARY KEY,
        timestamp DATETIME NOT NULL,
        device_address TINYINT UNSIGNED,
        temperature FLOAT COMMENT '温度(°C)',
        x_speed INT COMMENT 'X轴速度(mg)',
        y_speed INT COMMENT 'Y轴速度(mg)',
        z_speed INT COMMENT 'Z轴速度(mg)',
        x_displacement INT COMMENT 'X轴位移(mg)',
        y_displacement INT COMMENT 'Y轴位移(mg)',
        z_displacement INT COMMENT 'Z轴位移(mg)',
        reserved1 INT DEFAULT 0,
        reserved2 INT DEFAULT 0,
        reserved3 INT DEFAULT 0
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    try:
        cursor = conn.cursor()
        cursor.execute(create_table_query)
        print("数据库表初始化完成")
        cursor.close()
    except Error as e:
        print(f"数据库初始化错误: {e}")

# 保存数据到数据库
def save_to_database(conn, address, registers):
    """
    将传感器数据保存到数据库
    """
    if conn is None:
        return False
        
    try:
        insert_query = """
        INSERT INTO sensor_data (
            timestamp,
            device_address,
            temperature,
            x_speed,
            y_speed,
            z_speed,
            x_displacement,
            y_displacement,
            z_displacement,
            reserved1,
            reserved2,
            reserved3
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        # 准备数据值
        current_time = datetime.now()
        temperature = registers[0] / 10.0  # 温度值转换
        data_values = (
            current_time,           # 当前时间戳
            address,                # 设备地址
            temperature,             # 温度 (°C)
            registers[1],           # x_speed (mg)
            registers[2],           # y_speed (mg)
            registers[3],           # z_speed (mg)
            registers[4],           # x_displacement (mg)
            registers[5],           # y_displacement (mg)
            registers[6],           # z_displacement (mg)
            registers[7],           # reserved1
            registers[8],           # reserved2
            registers[9]            # reserved3
        )
        
        # 执行插入操作
        cursor = conn.cursor()
        cursor.execute(insert_query, data_values)
        conn.commit()
        cursor.close()
        return True
    except Error as e:
        print(f"数据库插入错误: {e}")
        # 尝试重新连接
        try:
            conn.ping(reconnect=True)
            print("数据库连接已恢复")
        except:
            print("数据库连接失败，请检查配置")
        return False

# 初始化串口连接
def connect_serial(port):
    """
    初始化串口连接
    """
    try:
        ser = serial.Serial(
            port=port,
            baudrate=9600,
            parity=serial.PARITY_NONE,
            bytesize=serial.EIGHTBITS,
            stopbits=serial.STOPBITS_ONE,
            timeout=1,
            rtscts=False
        )
        print(f"成功连接串口 {port}")
        return ser
    except serial.SerialException as e:
        print(f"连接串口 {port} 失败: {e}")
        sys.exit(1)

# 发送Modbus请求
def send_modbus_request(ser, slave_address=1, function_code=3, start_address=0, reg_count=10):
    """
    生成并发送MODBUS RTU读取保持寄存器请求
    返回: 完整的请求字节串
    """
    # 构造Modbus查询帧 (8字节)
    request = struct.pack('>B B H H', slave_address, function_code, start_address, reg_count)
    
    # 计算CRC (小端)
    crc = crc16_modbus(request)
    request += struct.pack('<H', crc)  # MODBUS使用小端CRC
    
    # 发送请求
    ser.reset_input_buffer()
    ser.reset_output_buffer()
    ser.write(request)
    
    print(f"已发送: {binascii.hexlify(request, ' ').decode('utf-8').upper()}")
    return request

# 解析Modbus响应
def parse_modbus_response(response):
    """
    解析MODBUS响应并返回结果数据
    返回: (成功状态, 地址, 功能码, 寄存器值列表)
    """
    # 最小响应长度校验
    if len(response) < 5:
        print(f"响应过短({len(response)}字节)，无法解析")
        return False, None, None, None
    
    # 提取头部信息
    address = response[0]
    function_code = response[1]
    data_length = response[2]
    
    # 错误处理 (功能码包含异常标志)
    if function_code & 0x80:
        error_code = response[2]
        print(f"设备返回错误: 功能码 {function_code & 0x7F}, 错误码 {error_code}")
        return False, address, function_code, None
    
    # 校验响应长度
    expected_len = 5 + data_length  # 地址+功能码+长度+数据+CRC
    if len(response) != expected_len:
        print(f"长度不匹配: 预期 {expected_len} 字节, 实际 {len(response)} 字节")
        return False, address, function_code, None
    
    # 校验CRC
    crc_received = struct.unpack('<H', response[-2:])[0]  # 小端CRC
    crc_calculated = crc16_modbus(response[:-2])
    
    if crc_received != crc_calculated:
        print(f"CRC校验失败! 接收值: {crc_received:04X}, 计算值: {crc_calculated:04X}")
        return False, address, function_code, None
    
    # 提取数据部分并解包寄存器值
    try:
        data_block = response[3:-2]  # 去掉头尾保留数据部分
        register_count = data_length // 2
        register_data = struct.unpack(f'>{register_count}H', data_block)
        return True, address, function_code, register_data
    except struct.error:
        print(f"数据解析失败: 数据长度 {data_length} 无效")
        return False, address, function_code, None

# 主程序
def main():
    # 配置参数
    PORT = 'COM7'
    UPDATE_INTERVAL = 3  # 更新频率(秒)
    
    # 初始化串口
    ser = connect_serial(PORT)
    
    # 初始化数据库连接
    db_conn = create_db_connection()
    if db_conn:
        initialize_database(db_conn)
    
    # 字段名称
    DATA_LABELS = ["温度", "X轴速度", "Y轴速度", "Z轴速度", 
                "X轴位移", "Y轴位移", "Z轴位移", 
                "预留1", "预留2", "预留3"]
    UNITS = ["°C", "mg", "mg", "mg", "mg", "mg", "mg", "", "", ""]
    
    try:
        print(f"开始读取数据 (每 {UPDATE_INTERVAL} 秒更新一次) - 按 Ctrl+C 退出")
        print("-" * 50)
        
        # 统计信息
        successful_reads = 0
        database_inserts = 0
        
        # 主循环
        while True:
            start_time = time.time()
            
            # 发送请求并获取响应
            request_data = send_modbus_request(ser)
            time.sleep(0.1)  # 等待设备响应
            
            # 检查响应数据
            in_waiting = ser.in_waiting
            if in_waiting == 0:
                print("警告: 未收到响应数据")
                time.sleep(UPDATE_INTERVAL)
                continue
                
            # 读取完整响应
            response = ser.read(in_waiting)
            
            # 解析响应
            success, addr, func_code, registers = parse_modbus_response(response)
            
            # 处理解析结果
            if success and registers:
                successful_reads += 1
                
                # 打印结果
                print(f"地址: {addr} | 功能码: {func_code}")
                for i in range(min(len(DATA_LABELS), len(registers))):
                    # 特殊处理温度值
                    value = registers[i] / 10.0 if i == 0 else registers[i]
                    print(f"{DATA_LABELS[i]}: {value} {UNITS[i]}")
                print("-" * 30)
                
                # 存储到数据库
                if db_conn:
                    if save_to_database(db_conn, addr, registers):
                        database_inserts += 1
                        print(f"数据已存储到数据库 (总数: {database_inserts})")
            
            # 显示统计信息
            elapsed = time.time() - start_time
            print(f"成功读取: {successful_reads}, 数据库存储: {database_inserts}")
            print(f"循环耗时: {elapsed:.3f}秒")
            
            # 计算并等待剩余时间
            if elapsed < UPDATE_INTERVAL:
                wait_time = UPDATE_INTERVAL - elapsed
                print(f"等待 {wait_time:.3f}秒...")
                time.sleep(wait_time)
            else:
                print("警告: 循环超时!")
                
    except KeyboardInterrupt:
        print("\n用户中断程序")
    finally:
        # 清理资源
        ser.close()
        print("串口已关闭")
        
        # 关闭数据库连接
        if db_conn and db_conn.is_connected():
            db_conn.close()
            print("数据库连接已关闭")

# 程序入口
if __name__ == "__main__":
    main()