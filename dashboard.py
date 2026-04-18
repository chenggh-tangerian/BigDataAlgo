import os
from datetime import datetime
import pandas as pd
import redis
import streamlit as st
import plotly.express as px
from streamlit_autorefresh import st_autorefresh

REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB = 0
TRIGGER_FILE = "trigger.txt"

def redis_client() -> redis.Redis:
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)

def parse_hot_topics(client: redis.Redis):
    raw = client.hgetall("hot_topics")
    result = []
    for word, count in raw.items():
        try:
            result.append((word, int(count)))
        except ValueError:
            continue
    # 反向展示：按从大到小排序
    result.sort(key=lambda item: item[1], reverse=True)
    return result

def apply_config(client: redis.Redis, k: int, n: int) -> None:
    client.hset("control_config", mapping={"k": k, "n": n})

def reset_system(client: redis.Redis) -> None:
    client.set("control_reset", "1")


KEYWORD_CATEGORY_MAP = {
    "AI": "AI/模型",
    "Agent": "AI/模型",
    "Copilot": "AI/模型",
    "NLP": "AI/模型",
    "Spark": "数据基础设施",
    "Kafka": "数据基础设施",
    "Redis": "数据基础设施",
    "DataLake": "数据基础设施",
    "VectorDB": "数据基础设施",
    "BigData": "数据基础设施",
    "Streaming": "实时计算",
    "Realtime": "实时计算",
    "Search": "搜索与推荐",
    "Ranking": "搜索与推荐",
    "Index": "搜索与推荐",
    "Analytics": "分析洞察",
    "Python": "工程生态",
    "Docker": "工程生态",
    "GPU": "工程生态",
    "Cloud": "工程生态",
}

# ================= 计算内存消耗的“装逼”函数 =================
def format_bytes(size: float) -> str:
    # 格式化字节大小
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024.0:
            return f"{size:.2f} {unit}"
        size /= 1024.0

def calculate_memory_metrics(total_processed: int, k_value: int):
    # 假设平均每个字符串 15 Bytes，Python 字典每个 Entry 约 80 Bytes，总计单条数据算 100 Bytes
    BYTES_PER_ENTRY = 100 
    
    # 【传统 Hash Map】: 假设长尾分布下，20% 的词是独一无二的
    unique_words_estimate = total_processed * 0.2
    exact_memory = unique_words_estimate * BYTES_PER_ENTRY
    
    # 【Misra-Gries】: 永远只存 K 个
    mg_memory = k_value * BYTES_PER_ENTRY
    
    # 节约比例
    if exact_memory > 0:
        saved_percent = ((exact_memory - mg_memory) / exact_memory) * 100
    else:
        saved_percent = 0.0
        
    return format_bytes(exact_memory), format_bytes(mg_memory), min(saved_percent, 99.99)


def calculate_operational_metrics(hot_topics, samples, last_update_epoch: int):
    total_hot = sum(count for _, count in hot_topics)
    top1_count = max((count for _, count in hot_topics), default=0)
    top1_ratio = (top1_count / total_hot) if total_hot > 0 else 0.0
    sample_diversity = (len(set(samples)) / len(samples)) if samples else 0.0
    data_freshness_seconds = max(int(datetime.now().timestamp()) - last_update_epoch, 0) if last_update_epoch else -1
    return top1_ratio, sample_diversity, data_freshness_seconds


def build_category_frame(hot_topics):
    category_counter = {}
    for keyword, count in hot_topics:
        category = KEYWORD_CATEGORY_MAP.get(keyword, "其他")
        category_counter[category] = category_counter.get(category, 0) + count
    if not category_counter:
        return pd.DataFrame(columns=["category", "count"])
    return pd.DataFrame(
        sorted(category_counter.items(), key=lambda item: item[1], reverse=True),
        columns=["category", "count"],
    )

# ================= 渲染水库抽样标签云 =================
def render_tag_cloud(samples):
    if not samples:
        st.info("水库蓄水中...")
        return
        
    # 用 HTML/CSS 替代死板的表格
    css = """
    <style>
    .tag-container { display: flex; flex-wrap: wrap; gap: 10px; padding: 10px 0; }
    .tag { background-color: #1E1E2E; border: 1px solid #3E3E5E; border-radius: 20px; 
           padding: 6px 14px; font-size: 14px; color: #E0E0E0; font-family: monospace;
           box-shadow: 0 2px 4px rgba(0,0,0,0.2); transition: all 0.3s ease; }
    .tag:hover { border-color: #00FFAA; color: #00FFAA; transform: scale(1.05); }
    </style>
    """
    tags_html = "".join([f'<div class="tag">{s}</div>' for s in samples])
    html_str = f"{css}<div class='tag-container'>{tags_html}</div>"
    st.markdown(html_str, unsafe_allow_html=True)


def main() -> None:
    st.set_page_config(page_title="亚线性流处理看板", layout="wide", initial_sidebar_state="expanded")
    
    # 自定义全局 CSS，让界面更有极客感
    st.markdown("""
        <style>
        .block-container { padding-top: 2rem; padding-bottom: 2rem; }
        h1 { color: #FFFFFF; font-weight: 600; }
        .stMetric label { color: #A0A0B0 !important; font-size: 1rem !important; }
        </style>
    """, unsafe_allow_html=True)

    st.title("🌊 基于亚线性算法的超大规模流处理系统")
    st.caption("Architecture: Spark Structured Streaming x Misra-Gries x Reservoir Sampling | Storage: Redis")

    client = redis_client()

    # 读取侧边栏控制状态
    with st.sidebar:
        st.header("⚙️ 引擎控制台")
        refresh_seconds = st.slider("🔄 监控刷新频率 (秒)", min_value=1, max_value=5, value=2)
        st_autorefresh(interval=refresh_seconds * 1000, key="refresh")

        st.divider()
        st.subheader("🧮 亚线性参数")
        k_value = st.number_input("MG 算法槽位 (K)", min_value=10, max_value=500, value=50, step=10)
        n_value = st.number_input("水库容量 (N)", min_value=5, max_value=200, value=20, step=5)
        if st.button("立即应用参数", use_container_width=True):
            apply_config(client, int(k_value), int(n_value))
            st.toast("✅ 参数已下发至 Spark 集群")

        st.divider()
        # =============== 交互式“爆发模式”控制 ===============
        st.subheader("🚀 模拟网络风暴")
        burst_word = st.text_input("目标引爆词汇", value="VibeCoding")
        
        is_bursting = os.path.exists(TRIGGER_FILE)
        
        if not is_bursting:
            if st.button("🔥 发起流量攻击", type="primary", use_container_width=True):
                with open(TRIGGER_FILE, "w") as f:
                    f.write(burst_word)
                st.rerun()
        else:
            st.error("🚨 遭受攻击中...")
            if st.button("🛑 停止攻击", use_container_width=True):
                if os.path.exists(TRIGGER_FILE):
                    os.remove(TRIGGER_FILE)
                st.rerun()

        st.divider()
        if st.button("🗑️ 重置集群状态", use_container_width=True):
            reset_system(client)
            st.toast("已发送重置信号")

    # 获取数据
    hot_topics = parse_hot_topics(client)
    samples = client.lrange("samples", 0, -1)
    total_processed = int(client.get("total_processed") or 0)
    last_batch_count = int(client.get("last_batch_count") or 0)
    last_update_epoch = int(client.get("last_update_epoch") or 0)
    
    # ================= 顶部 Metric (装逼核心区域) =================
    exact_mem, mg_mem, saved_pct = calculate_memory_metrics(total_processed, int(k_value))
    top1_ratio, sample_diversity, freshness_seconds = calculate_operational_metrics(hot_topics, samples, last_update_epoch)
    
    # 判断状态灯
    status_light = "🚨 **网络风暴 (Burst)**" if os.path.exists(TRIGGER_FILE) else "🟢 **正常负载**"

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("累计吞吐量 (Messages)", f"{total_processed:,}")
    with c2:
        st.metric("实时吞吐率 (Batch Size)", f"{last_batch_count:,}")
    with c3:
        st.metric("预估传统内存消耗 (Exact)", exact_mem, help="如果使用精确 Hash Map 所需的内存估算")
    with c4:
        # 使用 inverse=True 让负数（下降）显示为绿色！这是神来之笔。
        st.metric(f"MG 算法内存消耗 (K={k_value})", mg_mem, delta=f"-{saved_pct:.2f}% (内存节省)", delta_color="inverse")

    # ================= 业务可用性看板（实用性增强） =================
    st.subheader("🧭 业务可用性指标")
    o1, o2, o3 = st.columns(3)
    with o1:
        st.metric("热词集中度 (Top1 / TopK)", f"{top1_ratio * 100:.1f}%")
    with o2:
        st.metric("样本多样性 (Unique / N)", f"{sample_diversity * 100:.1f}%")
    with o3:
        if freshness_seconds >= 0:
            st.metric("数据新鲜度", f"{freshness_seconds}s")
        else:
            st.metric("数据新鲜度", "-")

    alert_msgs = []
    if freshness_seconds >= max(6, refresh_seconds * 3):
        alert_msgs.append("数据更新延迟偏高：可能出现处理链路阻塞")
    if top1_ratio >= 0.85 and total_processed > 1000:
        alert_msgs.append("热词集中度过高：可能是突发事件或异常流量")
    if sample_diversity <= 0.35 and len(samples) >= 10:
        alert_msgs.append("样本多样性偏低：建议检查词源分布或刷量行为")

    if alert_msgs:
        for msg in alert_msgs:
            st.warning(f"⚠️ {msg}")
    else:
        st.success("✅ 当前运行稳定，可用于实时趋势观察与异常发现")

    st.markdown("---")

    # ================= 主体图表区 =================
    left, right = st.columns([2, 1])

    with left:
        st.subheader("🔥 实时热搜榜 (Misra-Gries 近似估计)")
        if hot_topics:
            top_df = pd.DataFrame(hot_topics, columns=["keyword", "count"])
            
            # 动态高亮逻辑：如果在爆发模式，且当前词是目标词，则显示亮红色，否则显示蓝色
            if os.path.exists(TRIGGER_FILE):
                target_word = open(TRIGGER_FILE).read().strip()
                top_df['color'] = top_df['keyword'].apply(lambda x: '#FF4B4B' if x == target_word else '#3498DB')
            else:
                top_df['color'] = '#3498DB'

            # 使用 Plotly 绘制横向柱状图
            fig = px.bar(
                top_df, 
                x="count", 
                y="keyword", 
                orientation='h',
                text="count"
            )
            fig.update_traces(
                marker_color=top_df['color'], 
                textposition='outside',
                texttemplate='%{text:.2s}' # 大数字格式化，比如 15k
            )
            fig.update_layout(
                margin=dict(l=0, r=0, t=10, b=0),
                xaxis_title=None,
                yaxis_title=None,
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
                height=450,
                xaxis=dict(showgrid=True, gridcolor='#333333'),
                yaxis=dict(autorange="reversed") # 反转 Y 轴顺序展示
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("等待数据流接入中...")

        category_df = build_category_frame(hot_topics)
        st.subheader("📦 主题分类占比 (业务视角)")
        if not category_df.empty:
            pie = px.pie(category_df, names="category", values="count", hole=0.45)
            pie.update_layout(margin=dict(l=0, r=0, t=10, b=0), height=320)
            st.plotly_chart(pie, use_container_width=True)
        else:
            st.info("暂无可分类数据")

    with right:
        # 修改为极具业务价值的命名：
        st.subheader("📡 全局流量探针 (Reservoir Sampling)")
        st.caption("用途：为下游 ML 团队提供 O(1) 空间复杂度的无偏训练集")
        # 渲染高级标签云
        render_tag_cloud(samples)

if __name__ == "__main__":
    main()