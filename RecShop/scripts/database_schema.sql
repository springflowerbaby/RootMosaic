-- RecShop schema for a new dedicated MySQL database.
-- Existing databases are not initialized by the environment startup scripts.
-- model_metrics seed rows are illustrative values, not measured research results.
CREATE DATABASE IF NOT EXISTS shopify2
DEFAULT CHARACTER SET utf8mb4
COLLATE utf8mb4_unicode_ci;

USE shopify2;

-- ============================================================
-- 1. 用户表 (users)
-- ============================================================
CREATE TABLE IF NOT EXISTS users (
    id INT AUTO_INCREMENT PRIMARY KEY COMMENT '自增主键',
    user_token VARCHAR(100) UNIQUE NOT NULL COMMENT '用户唯一标识（对应数据集中的user_id）',
    username VARCHAR(100) COMMENT '用户名（展示用）',
    email VARCHAR(255) COMMENT '邮箱',
    password_hash VARCHAR(255) COMMENT '密码哈希',
    avatar_url VARCHAR(500) COMMENT '头像URL',
    status ENUM('active', 'banned') NOT NULL DEFAULT 'active' COMMENT '用户状态：active=正常, banned=已封禁',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    INDEX idx_user_token (user_token),
    INDEX idx_email (email)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='用户表';

-- ============================================================
-- 2. 商品表 (items)
-- ============================================================
CREATE TABLE IF NOT EXISTS items (
    id INT AUTO_INCREMENT PRIMARY KEY COMMENT '自增主键',
    item_id VARCHAR(100) UNIQUE NOT NULL COMMENT '商品ID（Amazon ASIN或ISBN）',
    title VARCHAR(500) COMMENT '商品标题',
    category VARCHAR(200) COMMENT '商品类别',
    brand VARCHAR(200) COMMENT '品牌',
    price DECIMAL(10, 2) COMMENT '价格',
    image_url VARCHAR(500) COMMENT '商品图片URL',
    description TEXT COMMENT '商品描述',
    rating DECIMAL(3, 2) COMMENT '平均评分',
    review_count INT DEFAULT 0 COMMENT '评论数量',
    merchant_id INT DEFAULT NULL COMMENT '所属商家ID（NULL=平台自营/导入数据）',
    status ENUM('draft', 'pending', 'active', 'rejected', 'removed') NOT NULL DEFAULT 'active'
        COMMENT '商品状态：draft=草稿, pending=待审核, active=上架, rejected=已拒绝, removed=已下架',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    INDEX idx_item_id (item_id),
    INDEX idx_category (category),
    INDEX idx_brand (brand),
    INDEX idx_merchant_id (merchant_id),
    INDEX idx_status (status),
    FULLTEXT idx_title (title)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='商品表';

-- ============================================================
-- 3. 用户-商品交互表 (interactions)
-- ============================================================
CREATE TABLE IF NOT EXISTS interactions (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '自增主键',
    user_token VARCHAR(100) NOT NULL COMMENT '用户标识',
    item_id VARCHAR(100) NOT NULL COMMENT '商品ID',
    interaction_type ENUM('view', 'click', 'purchase', 'rating', 'add_to_cart', 'remove_from_cart') DEFAULT 'view' COMMENT '交互类型',
    rating DECIMAL(2, 1) COMMENT '评分（1-5）',
    duration INT COMMENT '停留时长（秒，用于view类型）',
    quantity INT DEFAULT 1 COMMENT '数量（用于purchase类型）',
    price DECIMAL(10, 2) COMMENT '交易价格（用于purchase类型）',
    source VARCHAR(50) COMMENT '来源（推荐/搜索/浏览/直接访问）',
    session_id VARCHAR(100) COMMENT '会话ID',
    timestamp BIGINT NOT NULL COMMENT '交互时间戳（毫秒）',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
    INDEX idx_user_token (user_token),
    INDEX idx_item_id (item_id),
    INDEX idx_timestamp (timestamp),
    INDEX idx_user_time (user_token, timestamp),
    INDEX idx_session_id (session_id),
    FOREIGN KEY (user_token) REFERENCES users(user_token) ON DELETE CASCADE,
    FOREIGN KEY (item_id) REFERENCES items(item_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='用户-商品交互表';

-- ============================================================
-- 4. 推荐记录表 (recommendations)
-- ============================================================
CREATE TABLE IF NOT EXISTS recommendations (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '自增主键',
    user_token VARCHAR(100) NOT NULL COMMENT '用户标识',
    item_id VARCHAR(100) NOT NULL COMMENT '推荐的商品ID',
    score DECIMAL(10, 6) NOT NULL COMMENT '推荐得分',
    `rank` INT NOT NULL COMMENT '推荐排名',
    input_sequence TEXT COMMENT '输入序列（JSON格式）',
    model_version VARCHAR(50) DEFAULT 'SASRec-v1' COMMENT '模型版本',
    inference_time DECIMAL(10, 3) COMMENT '推理耗时（秒）',
    is_clicked BOOLEAN DEFAULT FALSE COMMENT '是否被点击',
    is_purchased BOOLEAN DEFAULT FALSE COMMENT '是否被购买',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '推荐时间',
    INDEX idx_user_token (user_token),
    INDEX idx_created_at (created_at),
    INDEX idx_user_time (user_token, created_at),
    FOREIGN KEY (user_token) REFERENCES users(user_token) ON DELETE CASCADE,
    FOREIGN KEY (item_id) REFERENCES items(item_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='推荐记录表';

-- ============================================================
-- 5. 模型性能指标表 (model_metrics)
-- ============================================================
CREATE TABLE IF NOT EXISTS model_metrics (
    id INT AUTO_INCREMENT PRIMARY KEY COMMENT '自增主键',
    model_name VARCHAR(100) NOT NULL COMMENT '模型名称',
    model_version VARCHAR(50) NOT NULL COMMENT '模型版本',
    dataset_name VARCHAR(100) COMMENT '数据集名称',
    metric_name VARCHAR(50) NOT NULL COMMENT '指标名称（NDCG@10, Recall@10等）',
    metric_value DECIMAL(10, 6) NOT NULL COMMENT '指标值',
    epoch INT COMMENT '训练轮数',
    training_time INT COMMENT '训练时长（秒）',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '记录时间',
    INDEX idx_model_version (model_version),
    INDEX idx_metric_name (metric_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='模型性能指标表';

-- ============================================================
-- 6. 用户会话表 (sessions) - 用于追踪用户浏览行为
-- ============================================================
CREATE TABLE IF NOT EXISTS sessions (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '自增主键',
    session_id VARCHAR(100) UNIQUE NOT NULL COMMENT '会话ID',
    user_token VARCHAR(100) COMMENT '用户标识（可为空，支持匿名用户）',
    start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '会话开始时间',
    end_time TIMESTAMP NULL COMMENT '会话结束时间',
    page_views INT DEFAULT 0 COMMENT '页面浏览数',
    item_views INT DEFAULT 0 COMMENT '商品浏览数',
    recommendations_shown INT DEFAULT 0 COMMENT '展示的推荐数',
    recommendations_clicked INT DEFAULT 0 COMMENT '点击的推荐数',
    INDEX idx_session_id (session_id),
    INDEX idx_user_token (user_token),
    INDEX idx_start_time (start_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='用户会话表';

-- ============================================================
-- 7. 购物车表 (cart_items)
-- ============================================================
CREATE TABLE IF NOT EXISTS cart_items (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '自增主键',
    user_token VARCHAR(100) NOT NULL COMMENT '用户标识',
    item_id VARCHAR(100) NOT NULL COMMENT '商品ID',
    quantity INT NOT NULL DEFAULT 1 COMMENT '商品数量',
    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '添加到购物车时间',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '最后更新时间',
    UNIQUE KEY uk_user_item (user_token, item_id) COMMENT '同一用户的同一商品唯一',
    INDEX idx_user_token (user_token),
    INDEX idx_item_id (item_id),
    INDEX idx_added_at (added_at),
    CONSTRAINT fk_cart_user FOREIGN KEY (user_token) REFERENCES users(user_token) ON DELETE CASCADE,
    CONSTRAINT fk_cart_item FOREIGN KEY (item_id) REFERENCES items(item_id) ON DELETE CASCADE,
    CONSTRAINT chk_quantity CHECK (quantity > 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='购物车表';

-- ============================================================
-- 8. 收货地址表 (user_addresses)
-- ============================================================
CREATE TABLE IF NOT EXISTS user_addresses (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '地址ID',
    user_token VARCHAR(100) NOT NULL COMMENT '用户标识',
    
    -- 收货人信息
    receiver_name VARCHAR(100) NOT NULL COMMENT '收货人姓名',
    receiver_phone VARCHAR(20) NOT NULL COMMENT '收货人电话',
    
    -- 地址信息
    province VARCHAR(50) COMMENT '省份',
    city VARCHAR(50) COMMENT '城市',
    district VARCHAR(50) COMMENT '区/县',
    address VARCHAR(500) NOT NULL COMMENT '详细地址',
    
    -- 标记
    is_default TINYINT(1) DEFAULT 0 COMMENT '是否默认地址（0=否，1=是）',
    
    -- 时间戳
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    
    -- 索引
    INDEX idx_user_token (user_token),
    INDEX idx_default (user_token, is_default),
    
    -- 外键
    CONSTRAINT fk_address_user FOREIGN KEY (user_token) 
        REFERENCES users(user_token) ON DELETE CASCADE
        
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='用户收货地址表';

-- ============================================================
-- 9. 订单主表 (orders)
-- ============================================================
CREATE TABLE IF NOT EXISTS orders (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '订单ID',
    order_no VARCHAR(50) UNIQUE NOT NULL COMMENT '订单号（唯一）',
    user_token VARCHAR(100) NOT NULL COMMENT '用户标识',
    
    -- 收货地址（关联地址表）
    address_id BIGINT NOT NULL COMMENT '收货地址ID',
    
    -- 收货信息快照（下单时的地址快照，防止地址修改影响历史订单）
    snapshot_receiver_name VARCHAR(100) NOT NULL COMMENT '收货人姓名（快照）',
    snapshot_receiver_phone VARCHAR(20) NOT NULL COMMENT '收货人电话（快照）',
    snapshot_address VARCHAR(500) NOT NULL COMMENT '收货地址（快照）',
    
    -- 订单金额
    total_amount DECIMAL(10, 2) NOT NULL COMMENT '订单总金额',
    item_count INT NOT NULL DEFAULT 0 COMMENT '商品总件数',
    
    -- 订单状态
    status ENUM('pending', 'paid', 'shipped', 'completed', 'cancelled') 
        NOT NULL DEFAULT 'pending' 
        COMMENT '订单状态：pending=待付款, paid=已付款, shipped=已发货, completed=已完成, cancelled=已取消',
    
    -- 备注信息
    remark TEXT COMMENT '订单备注（用户留言）',
    cancel_reason VARCHAR(200) COMMENT '取消原因',
    
    -- 时间戳
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    paid_at TIMESTAMP NULL COMMENT '付款时间',
    shipped_at TIMESTAMP NULL COMMENT '发货时间',
    completed_at TIMESTAMP NULL COMMENT '完成时间',
    cancelled_at TIMESTAMP NULL COMMENT '取消时间',
    
    -- 索引
    INDEX idx_user_token (user_token),
    INDEX idx_order_no (order_no),
    INDEX idx_status (status),
    INDEX idx_created_at (created_at),
    INDEX idx_address_id (address_id),
    
    -- 外键
    CONSTRAINT fk_order_user FOREIGN KEY (user_token) 
        REFERENCES users(user_token) ON DELETE CASCADE,
    CONSTRAINT fk_order_address FOREIGN KEY (address_id) 
        REFERENCES user_addresses(id) ON DELETE RESTRICT
        
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='订单表';

-- ============================================================
-- 10. 订单明细表 (order_items)
-- ============================================================
CREATE TABLE IF NOT EXISTS order_items (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '自增主键',
    order_id BIGINT NOT NULL COMMENT '订单ID',
    item_id VARCHAR(100) NOT NULL COMMENT '商品ID',
    
    -- 商品快照（下单时的商品信息，防止商品修改影响历史订单）
    item_title VARCHAR(500) NOT NULL COMMENT '商品标题（快照）',
    item_image VARCHAR(500) COMMENT '商品图片（快照）',
    item_price DECIMAL(10, 2) NOT NULL COMMENT '商品单价（快照）',
    
    -- 购买信息
    quantity INT NOT NULL DEFAULT 1 COMMENT '购买数量',
    subtotal DECIMAL(10, 2) NOT NULL COMMENT '小计金额（单价×数量）',
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    
    -- 索引
    INDEX idx_order_id (order_id),
    INDEX idx_item_id (item_id),
    
    -- 外键
    CONSTRAINT fk_order_item_order FOREIGN KEY (order_id) 
        REFERENCES orders(id) ON DELETE CASCADE,
    CONSTRAINT fk_order_item_item FOREIGN KEY (item_id) 
        REFERENCES items(item_id) ON DELETE CASCADE
        
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='订单明细表';

-- ============================================================
-- AI记忆表：存储AI助手从对话中提取的用户画像记忆
-- ============================================================
CREATE TABLE IF NOT EXISTS ai_memories (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_token VARCHAR(100) NOT NULL COMMENT '用户标识',
    memory_type ENUM('preference', 'need', 'constraint', 'personality') NOT NULL COMMENT '记忆类型',
    content VARCHAR(500) NOT NULL COMMENT '记忆内容（AI提炼的用户洞察）',
    source VARCHAR(200) DEFAULT NULL COMMENT '记忆来源（如 chat about ProductX）',
    confidence TINYINT DEFAULT 5 COMMENT '置信度 1-10',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_user (user_token),
    INDEX idx_user_type (user_token, memory_type)
    -- 注:实库 ai_memories 无外键约束(与真相源一致),故此处不声明 FK,仅保留上述索引。

) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='AI助手用户记忆表';

-- ============================================================
-- 12. 商家表 (merchants)
-- ============================================================
CREATE TABLE IF NOT EXISTS merchants (
    id INT PRIMARY KEY AUTO_INCREMENT,
    merchant_token VARCHAR(100) UNIQUE NOT NULL COMMENT '商家唯一标识',
    username VARCHAR(100) COMMENT '商家名称',
    email VARCHAR(255) UNIQUE NOT NULL COMMENT '登录邮箱',
    password_hash VARCHAR(255) NOT NULL COMMENT '密码哈希',
    phone VARCHAR(20) COMMENT '联系电话',
    status ENUM('pending', 'approved', 'rejected', 'banned') NOT NULL DEFAULT 'pending'
        COMMENT '状态：pending=待审核, approved=已通过, rejected=已拒绝, banned=已封禁',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    INDEX idx_email (email),
    INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='商家表';

-- ============================================================
-- 13. 商家店铺表 (shops)
-- ============================================================
CREATE TABLE IF NOT EXISTS shops (
    id INT PRIMARY KEY AUTO_INCREMENT,
    merchant_id INT NOT NULL COMMENT '关联商家ID',
    name VARCHAR(100) NOT NULL COMMENT '店铺名称',
    description TEXT COMMENT '店铺描述',
    logo_url VARCHAR(500) COMMENT '店铺Logo URL',
    status ENUM('active', 'closed') NOT NULL DEFAULT 'active' COMMENT '店铺状态',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    INDEX idx_merchant_id (merchant_id),
    CONSTRAINT fk_shop_merchant FOREIGN KEY (merchant_id) REFERENCES merchants(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='商家店铺表';

-- ============================================================
-- 14. 管理员表 (admins)
-- ============================================================
CREATE TABLE IF NOT EXISTS admins (
    id INT PRIMARY KEY AUTO_INCREMENT,
    username VARCHAR(50) UNIQUE NOT NULL COMMENT '管理员用户名',
    email VARCHAR(255) UNIQUE NOT NULL COMMENT '邮箱（登录凭证）',
    password_hash VARCHAR(255) NOT NULL COMMENT '密码哈希',
    role ENUM('super_admin', 'operation', 'finance', 'customer_service') NOT NULL DEFAULT 'operation'
        COMMENT '角色：super_admin=超级管理员, operation=运营, finance=财务, customer_service=客服',
    status ENUM('active', 'disabled') NOT NULL DEFAULT 'active' COMMENT '账号状态',
    last_login_at TIMESTAMP NULL COMMENT '最后登录时间',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    INDEX idx_username (username),
    INDEX idx_email (email),
    INDEX idx_role (role),
    INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='管理员表';

-- ============================================================
-- 15. 管理员操作日志表 (admin_logs)
-- ============================================================
CREATE TABLE IF NOT EXISTS admin_logs (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    admin_id INT NOT NULL COMMENT '操作管理员ID',
    action VARCHAR(100) NOT NULL COMMENT '操作类型（如 approve_merchant, ban_user）',
    target_type VARCHAR(50) COMMENT '操作对象类型（user/merchant/item/order）',
    target_id VARCHAR(100) COMMENT '操作对象ID',
    detail TEXT COMMENT '操作详情（JSON格式）',
    ip_address VARCHAR(45) COMMENT '操作IP',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '操作时间',
    INDEX idx_admin_id (admin_id),
    INDEX idx_action (action),
    INDEX idx_target (target_type, target_id),
    INDEX idx_created_at (created_at),
    CONSTRAINT fk_admin_log_admin FOREIGN KEY (admin_id) REFERENCES admins(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='管理员操作日志表';

-- ============================================================
-- 16. 平台公告表 (announcements)
-- ============================================================
CREATE TABLE IF NOT EXISTS announcements (
    id INT PRIMARY KEY AUTO_INCREMENT,
    title VARCHAR(200) NOT NULL COMMENT '公告标题',
    content TEXT COMMENT '公告内容',
    type ENUM('notice', 'banner', 'popup') NOT NULL DEFAULT 'notice' COMMENT '公告类型',
    image_url VARCHAR(500) COMMENT '图片URL（用于Banner）',
    link_url VARCHAR(500) COMMENT '跳转链接',
    sort_order INT DEFAULT 0 COMMENT '排序（越大越靠前）',
    status ENUM('draft', 'published', 'archived') NOT NULL DEFAULT 'draft' COMMENT '状态',
    published_at TIMESTAMP NULL COMMENT '发布时间',
    created_by INT COMMENT '创建管理员ID',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    INDEX idx_type_status (type, status),
    CONSTRAINT fk_announcement_admin FOREIGN KEY (created_by) REFERENCES admins(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='平台公告表';

-- ============================================================
-- 17. 商品评论表 (reviews)
-- ============================================================
CREATE TABLE IF NOT EXISTS reviews (
    id BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '评论ID',
    order_item_id BIGINT NOT NULL COMMENT '关联订单明细ID（确保购买后才能评价）',
    user_token VARCHAR(100) NOT NULL COMMENT '评论用户',
    item_id VARCHAR(100) NOT NULL COMMENT '评论商品',
    rating TINYINT NOT NULL COMMENT '评分（1-5）',
    content TEXT COMMENT '评论文字内容',
    images JSON COMMENT '评论图片URL数组（最多9张）',
    is_anonymous TINYINT(1) DEFAULT 0 COMMENT '是否匿名评价（0=否，1=是）',
    status ENUM('pending', 'approved', 'rejected', 'hidden') NOT NULL DEFAULT 'approved'
        COMMENT '评论状态：pending=待审核, approved=已通过, rejected=已拒绝, hidden=已隐藏',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '评论时间',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',

    UNIQUE KEY uk_order_item (order_item_id) COMMENT '同一订单明细只能评价一次',
    INDEX idx_item_id (item_id),
    INDEX idx_user_token (user_token),
    INDEX idx_status (status),
    INDEX idx_created_at (created_at),
    INDEX idx_item_status_time (item_id, status, created_at DESC),

    CONSTRAINT fk_review_order_item FOREIGN KEY (order_item_id)
        REFERENCES order_items(id) ON DELETE CASCADE,
    CONSTRAINT fk_review_user FOREIGN KEY (user_token)
        REFERENCES users(user_token) ON DELETE CASCADE,
    CONSTRAINT fk_review_item FOREIGN KEY (item_id)
        REFERENCES items(item_id) ON DELETE CASCADE,
    CONSTRAINT chk_rating CHECK (rating BETWEEN 1 AND 5)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='商品评论表';

-- ============================================================
-- 18. 商家评论回复表 (review_replies)
-- ============================================================
CREATE TABLE IF NOT EXISTS review_replies (
    id BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '回复ID',
    review_id BIGINT NOT NULL COMMENT '关联评论ID',
    merchant_id INT NOT NULL COMMENT '回复商家ID',
    content TEXT NOT NULL COMMENT '回复内容',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '回复时间',

    UNIQUE KEY uk_review (review_id) COMMENT '每条评论商家只能回复一次',
    INDEX idx_merchant_id (merchant_id),

    CONSTRAINT fk_reply_review FOREIGN KEY (review_id)
        REFERENCES reviews(id) ON DELETE CASCADE,
    CONSTRAINT fk_reply_merchant FOREIGN KEY (merchant_id)
        REFERENCES merchants(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='商家评论回复表';

-- ============================================================
-- 19. 库存表 (inventory) —— batch2 微服务拆分新增
--     由库存/履约相关服务维护，item_id 为商品ID（无外键，弱耦合内网表）
--     注意：本表由 MySQL 8 默认排序规则创建，使用 utf8mb4_0900_ai_ci
-- ============================================================
CREATE TABLE IF NOT EXISTS inventory (
    item_id VARCHAR(64) NOT NULL COMMENT '商品ID',
    stock INT NOT NULL DEFAULT 0 COMMENT '可用库存',
    reserved INT NOT NULL DEFAULT 0 COMMENT '已预占库存（下单未支付等）',
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (item_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='库存表';

-- ============================================================
-- 20. 价格规则表 (price_rules) —— batch2 微服务拆分新增
--     定价服务用于按商品计算加价/税率（无外键，弱耦合内网表）
--     注意：本表由 MySQL 8 默认排序规则创建，使用 utf8mb4_0900_ai_ci
-- ============================================================
CREATE TABLE IF NOT EXISTS price_rules (
    item_id VARCHAR(64) NOT NULL COMMENT '商品ID',
    markup DECIMAL(10, 4) NOT NULL DEFAULT 0.0000 COMMENT '加价率/加价金额',
    tax_rate DECIMAL(10, 4) NOT NULL DEFAULT 0.0000 COMMENT '税率',
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (item_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='价格规则表';

-- ============================================================
-- 21. 支付记录表 (payments) —— batch2 微服务拆分新增
--     支付服务记录每笔订单的支付状态（按 order_no 关联订单，无外键约束）
--     注意：本表由 MySQL 8 默认排序规则创建，使用 utf8mb4_0900_ai_ci
-- ============================================================
CREATE TABLE IF NOT EXISTS payments (
    id INT NOT NULL AUTO_INCREMENT,
    order_no VARCHAR(64) NOT NULL COMMENT '关联订单号',
    amount DECIMAL(10, 2) NOT NULL DEFAULT 0.00 COMMENT '支付金额',
    status ENUM('pending', 'paid', 'refunded') NOT NULL DEFAULT 'pending' COMMENT '支付状态：pending=待支付, paid=已支付, refunded=已退款',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    PRIMARY KEY (id),
    -- Z11: order_no 唯一(支付幂等 DB 层兜底,对照 reviews.uk_order_item;既有库由 scripts/_archive/sql_migrations/migrate_z11_payment_shipment_unique.sql 先去重再补)
    UNIQUE KEY uk_payments_order_no (order_no)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='支付记录表';

-- ============================================================
-- 22. 促销/优惠码表 (promotions) —— batch2 微服务拆分新增
--     营销服务维护优惠码及折扣（无外键，弱耦合内网表）
--     注意：本表由 MySQL 8 默认排序规则创建，使用 utf8mb4_0900_ai_ci
-- ============================================================
CREATE TABLE IF NOT EXISTS promotions (
    id INT NOT NULL AUTO_INCREMENT,
    code VARCHAR(64) NOT NULL COMMENT '优惠码（唯一）',
    discount DECIMAL(10, 2) NOT NULL DEFAULT 0.00 COMMENT '折扣金额',
    active TINYINT(1) NOT NULL DEFAULT 1 COMMENT '是否启用（0=否，1=是）',
    expires_at DATETIME DEFAULT NULL COMMENT '过期时间',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    PRIMARY KEY (id),
    UNIQUE KEY code (code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='促销/优惠码表';

-- ============================================================
-- 23. 发货单表 (shipments) —— 微服务拆分新增（shipping_service 5016 自有表）
--     发货服务建/查发货单，按 order_no 关联订单（无外键，弱耦合内网表）。
--     DDL 权威源：services/shipping_service/app.py 的启动幂等建表（CREATE TABLE IF NOT EXISTS）。
-- ============================================================
CREATE TABLE IF NOT EXISTS shipments (
    id INT AUTO_INCREMENT PRIMARY KEY,
    order_no VARCHAR(64) NOT NULL COMMENT '关联订单号',
    carrier VARCHAR(64) DEFAULT NULL COMMENT '承运商',
    tracking_no VARCHAR(64) DEFAULT NULL COMMENT '运单号',
    status ENUM('pending', 'shipped', 'delivered') NOT NULL DEFAULT 'pending' COMMENT '发货状态',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    -- Z11: order_no 唯一(防并发双发货;既有库由 scripts/_archive/sql_migrations/migrate_z11_payment_shipment_unique.sql 先去重再补)
    UNIQUE KEY uk_shipments_order_no (order_no)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='发货单表';

-- ============================================================
-- 24. 通知表 (notifications) —— 微服务拆分新增（notification_service 5021 自有叶子表）
--     通知服务创建/列出用户通知，被 order/payment/shipping fan-in（无外键，弱耦合内网表）。
--     DDL 权威源：services/notification_service/app.py 的启动幂等建表（CREATE TABLE IF NOT EXISTS）。
-- ============================================================
CREATE TABLE IF NOT EXISTS notifications (
    id          BIGINT       NOT NULL AUTO_INCREMENT,
    user_token  VARCHAR(255) NOT NULL COMMENT '用户标识',
    type        VARCHAR(64)  NOT NULL DEFAULT 'system' COMMENT '通知类型',
    title       VARCHAR(255) NOT NULL COMMENT '通知标题',
    content     TEXT         NULL COMMENT '通知内容',
    is_read     TINYINT      NOT NULL DEFAULT 0 COMMENT '是否已读（0=否，1=是）',
    created_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    PRIMARY KEY (id),
    KEY idx_user_token (user_token),
    KEY idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用户通知表';

-- ============================================================
-- 插入示例数据
-- ============================================================

-- 插入模型性能指标（仅在表为空时 seed，保证一键重跑幂等：再跑不累加示例行）
INSERT INTO model_metrics (model_name, model_version, dataset_name, metric_name, metric_value, epoch, training_time)
SELECT * FROM (
    SELECT 'SASRec' AS model_name, 'SASRec-Oct-16-2025' AS model_version, 'Electronics' AS dataset_name, 'NDCG@10' AS metric_name, 0.4925 AS metric_value, 44 AS epoch, 259200 AS training_time
    UNION ALL SELECT 'SASRec', 'SASRec-Oct-16-2025', 'Electronics', 'Recall@10', 0.6828, 44, 259200
    UNION ALL SELECT 'SASRec', 'SASRec-Oct-16-2025', 'Electronics', 'Hit@10', 0.6828, 44, 259200
    UNION ALL SELECT 'SASRec', 'SASRec-Oct-16-2025', 'Electronics', 'Precision@10', 0.0683, 44, 259200
) AS seed
WHERE NOT EXISTS (SELECT 1 FROM model_metrics);

-- 创建默认管理员账号 (admin/admin123)：
-- 执行完此 SQL 后运行: python scripts/generate_admin.py

-- ============================================================
-- 视图：用户交互统计
-- ============================================================
CREATE OR REPLACE VIEW user_interaction_stats AS
SELECT 
    u.user_token,
    u.username,
    COUNT(i.id) as total_interactions,
    COUNT(DISTINCT i.item_id) as unique_items,
    MIN(i.timestamp) as first_interaction,
    MAX(i.timestamp) as last_interaction,
    AVG(CASE WHEN i.rating IS NOT NULL THEN i.rating END) as avg_rating
FROM users u
LEFT JOIN interactions i ON u.user_token = i.user_token
GROUP BY u.user_token, u.username;

-- ============================================================
-- 视图：商品热度统计
-- ============================================================
CREATE OR REPLACE VIEW item_popularity_stats AS
SELECT 
    it.item_id,
    it.title,
    it.category,
    it.brand,
    COUNT(i.id) as interaction_count,
    COUNT(DISTINCT i.user_token) as unique_users,
    AVG(CASE WHEN i.rating IS NOT NULL THEN i.rating END) as avg_rating,
    COUNT(CASE WHEN i.interaction_type = 'purchase' THEN 1 END) as purchase_count
FROM items it
LEFT JOIN interactions i ON it.item_id = i.item_id
GROUP BY it.item_id, it.title, it.category, it.brand;

-- ============================================================
-- 视图：推荐效果统计
-- ============================================================
CREATE OR REPLACE VIEW recommendation_performance AS
SELECT 
    DATE(created_at) as date,
    COUNT(*) as total_recommendations,
    COUNT(DISTINCT user_token) as unique_users,
    SUM(CASE WHEN is_clicked THEN 1 ELSE 0 END) as clicks,
    SUM(CASE WHEN is_purchased THEN 1 ELSE 0 END) as purchases,
    ROUND(SUM(CASE WHEN is_clicked THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as ctr,
    ROUND(SUM(CASE WHEN is_purchased THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as conversion_rate,
    AVG(inference_time) as avg_inference_time
FROM recommendations
GROUP BY DATE(created_at);

-- ============================================================
-- 视图：购物车详情（包含商品信息和小计）
-- ============================================================
CREATE OR REPLACE VIEW cart_details AS
SELECT 
    c.id,
    c.user_token,
    c.item_id,
    c.quantity,
    c.added_at,
    c.updated_at,
    i.title as item_title,
    i.category as item_category,
    i.brand as item_brand,
    i.price as item_price,
    i.image_url as item_image,
    (c.quantity * i.price) as subtotal
FROM cart_items c
JOIN items i ON c.item_id = i.item_id;

-- ============================================================
-- 视图：用户购物车统计
-- ============================================================
CREATE OR REPLACE VIEW cart_summary AS
SELECT 
    user_token,
    COUNT(*) as total_items,
    SUM(quantity) as total_quantity,
    SUM(quantity * i.price) as total_amount
FROM cart_items c
JOIN items i ON c.item_id = i.item_id
GROUP BY user_token;

-- ============================================================
-- 存储过程：获取用户最近的交互序列
-- ============================================================
DELIMITER //

DROP PROCEDURE IF EXISTS get_user_recent_interactions //

CREATE PROCEDURE get_user_recent_interactions(
    IN p_user_token VARCHAR(100),
    IN p_limit INT
)
BEGIN
    SELECT 
        i.item_id,
        it.title,
        it.image_url,
        i.interaction_type,
        i.timestamp,
        FROM_UNIXTIME(i.timestamp/1000) as interaction_time
    FROM interactions i
    JOIN items it ON i.item_id = it.item_id
    WHERE i.user_token = p_user_token
    ORDER BY i.timestamp DESC
    LIMIT p_limit;
END //

DELIMITER ;

-- ============================================================
-- 视图：订单汇总（含用户信息和商品统计）
-- ============================================================
CREATE OR REPLACE VIEW v_order_summary AS
SELECT 
    o.id,
    o.order_no,
    o.user_token,
    o.status,
    o.total_amount,
    o.item_count,
    o.snapshot_receiver_name,
    o.snapshot_receiver_phone,
    o.snapshot_address,
    o.remark,
    o.cancel_reason,
    o.created_at,
    o.paid_at,
    o.shipped_at,
    o.completed_at,
    o.cancelled_at,
    u.username,
    u.email,
    COUNT(oi.id) as detail_count,
    SUM(oi.quantity) as total_quantity
FROM orders o
LEFT JOIN users u ON o.user_token = u.user_token
LEFT JOIN order_items oi ON o.id = oi.order_id
GROUP BY o.id;

-- ============================================================
-- 视图：用户订单统计
-- ============================================================
CREATE OR REPLACE VIEW v_user_order_stats AS
SELECT 
    user_token,
    COUNT(*) as total_orders,
    SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending_orders,
    SUM(CASE WHEN status = 'paid' THEN 1 ELSE 0 END) as paid_orders,
    SUM(CASE WHEN status = 'shipped' THEN 1 ELSE 0 END) as shipped_orders,
    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed_orders,
    SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) as cancelled_orders,
    SUM(CASE WHEN status = 'completed' THEN total_amount ELSE 0 END) as total_spent,
    MAX(created_at) as last_order_time
FROM orders
GROUP BY user_token;

-- ============================================================
-- 视图：订单明细（含商品快照信息）
-- ============================================================
CREATE OR REPLACE VIEW v_order_item_details AS
SELECT 
    oi.id,
    oi.order_id,
    o.order_no,
    o.user_token,
    o.status as order_status,
    oi.item_id,
    oi.item_title,
    oi.item_image,
    oi.item_price,
    oi.quantity,
    oi.subtotal,
    oi.created_at,
    i.title as current_title,
    i.price as current_price
FROM order_items oi
JOIN orders o ON oi.order_id = o.id
LEFT JOIN items i ON oi.item_id = i.item_id;

-- ============================================================
-- 视图：平台概览统计（管理员仪表盘）
-- ============================================================
CREATE OR REPLACE VIEW v_platform_overview AS
SELECT
    (SELECT COUNT(*) FROM users) as total_users,
    (SELECT COUNT(*) FROM users WHERE created_at >= CURDATE()) as today_new_users,
    (SELECT COUNT(*) FROM merchants WHERE status = 'approved') as active_merchants,
    (SELECT COUNT(*) FROM merchants WHERE status = 'pending') as pending_merchants,
    (SELECT COUNT(*) FROM items WHERE status = 'active') as active_items,
    (SELECT COUNT(*) FROM orders) as total_orders,
    (SELECT COUNT(*) FROM orders WHERE DATE(created_at) = CURDATE()) as today_orders,
    (SELECT COALESCE(SUM(total_amount), 0) FROM orders WHERE status = 'completed') as total_gmv,
    (SELECT COALESCE(SUM(total_amount), 0) FROM orders 
     WHERE status = 'completed' AND DATE(completed_at) = CURDATE()) as today_gmv,
    (SELECT COUNT(*) FROM reviews) as total_reviews,
    (SELECT COUNT(*) FROM reviews WHERE status = 'pending') as pending_reviews;

-- ============================================================
-- 视图：每日订单统计（趋势图表）
-- ============================================================
CREATE OR REPLACE VIEW v_daily_order_stats AS
SELECT
    DATE(created_at) as date,
    COUNT(*) as order_count,
    SUM(total_amount) as total_amount,
    SUM(item_count) as total_items,
    COUNT(DISTINCT user_token) as unique_buyers,
    SUM(CASE WHEN status = 'completed' THEN total_amount ELSE 0 END) as completed_amount,
    SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) as cancelled_count
FROM orders
GROUP BY DATE(created_at)
ORDER BY date DESC;

-- ============================================================
-- 视图：商家销售统计（商家仪表盘）
-- ============================================================
CREATE OR REPLACE VIEW v_merchant_sales_stats AS
SELECT
    i.merchant_id,
    COUNT(DISTINCT i.id) as total_products,
    COUNT(DISTINCT CASE WHEN i.status = 'active' THEN i.id END) as active_products,
    COUNT(DISTINCT o.id) as total_orders,
    COALESCE(SUM(oi.subtotal), 0) as total_sales,
    COALESCE(SUM(oi.quantity), 0) as total_items_sold
FROM items i
LEFT JOIN order_items oi ON i.item_id = oi.item_id
LEFT JOIN orders o ON oi.order_id = o.id AND o.status IN ('paid', 'shipped', 'completed')
WHERE i.merchant_id IS NOT NULL
GROUP BY i.merchant_id;

-- ============================================================
-- 视图：商品评论统计（用于商品详情页和商家仪表盘）
-- ============================================================
CREATE OR REPLACE VIEW v_item_review_stats AS
SELECT
    r.item_id,
    COUNT(*) as review_count,
    ROUND(AVG(r.rating), 2) as avg_rating,
    SUM(CASE WHEN r.rating = 5 THEN 1 ELSE 0 END) as star_5,
    SUM(CASE WHEN r.rating = 4 THEN 1 ELSE 0 END) as star_4,
    SUM(CASE WHEN r.rating = 3 THEN 1 ELSE 0 END) as star_3,
    SUM(CASE WHEN r.rating = 2 THEN 1 ELSE 0 END) as star_2,
    SUM(CASE WHEN r.rating = 1 THEN 1 ELSE 0 END) as star_1,
    SUM(CASE WHEN r.images IS NOT NULL AND JSON_LENGTH(r.images) > 0 THEN 1 ELSE 0 END) as with_images,
    SUM(CASE WHEN rr.id IS NOT NULL THEN 1 ELSE 0 END) as replied_count
FROM reviews r
LEFT JOIN review_replies rr ON r.id = rr.review_id
WHERE r.status = 'approved'
GROUP BY r.item_id;

-- ============================================================
-- 视图：商家评论概览（用于商家端评论管理）
-- ============================================================
CREATE OR REPLACE VIEW v_merchant_review_stats AS
SELECT
    i.merchant_id,
    COUNT(r.id) as total_reviews,
    ROUND(AVG(r.rating), 2) as avg_rating,
    SUM(CASE WHEN rr.id IS NULL AND r.status = 'approved' THEN 1 ELSE 0 END) as pending_reply_count,
    SUM(CASE WHEN rr.id IS NOT NULL THEN 1 ELSE 0 END) as replied_count
FROM items i
JOIN reviews r ON i.item_id = r.item_id
LEFT JOIN review_replies rr ON r.id = rr.review_id
WHERE i.merchant_id IS NOT NULL
  AND r.status = 'approved'
GROUP BY i.merchant_id;

-- ============================================================
-- 存储过程：记录推荐结果
-- ============================================================
DELIMITER //

DROP PROCEDURE IF EXISTS save_recommendations //

CREATE PROCEDURE save_recommendations(
    IN p_user_token VARCHAR(100),
    IN p_recommendations JSON,
    IN p_input_sequence JSON,
    IN p_inference_time DECIMAL(10,3)
)
BEGIN
    DECLARE i INT DEFAULT 0;
    DECLARE rec_count INT;
    
    SET rec_count = JSON_LENGTH(p_recommendations);
    
    WHILE i < rec_count DO
        INSERT INTO recommendations (
            user_token, 
            item_id, 
            score, 
            `rank`, 
            input_sequence, 
            inference_time
        ) VALUES (
            p_user_token,
            JSON_UNQUOTE(JSON_EXTRACT(p_recommendations, CONCAT('$[', i, '].item_id'))),
            JSON_EXTRACT(p_recommendations, CONCAT('$[', i, '].score')),
            JSON_EXTRACT(p_recommendations, CONCAT('$[', i, '].rank')),
            p_input_sequence,
            p_inference_time
        );
        SET i = i + 1;
    END WHILE;
END //

DELIMITER ;
