// API配置
const API_BASE_URL = 'http://127.0.0.1:5001';

// DOM元素
const elements = {
    itemSequence: document.getElementById('itemSequence'),
    topK: document.getElementById('topK'),
    checkHealth: document.getElementById('checkHealth'),
    startRecommend: document.getElementById('startRecommend'),
    statusSection: document.getElementById('statusSection'),
    statusContent: document.getElementById('statusContent'),
    progressSection: document.getElementById('progressSection'),
    progressFill: document.getElementById('progressFill'),
    progressText: document.getElementById('progressText'),
    chatSection: document.getElementById('chatSection'),
    resultSection: document.getElementById('resultSection'),
    resultProductId: document.getElementById('resultProductId'),
    resultProductTitle: document.getElementById('resultProductTitle'),
    resultConfidence: document.getElementById('resultConfidence'),
    resultReason: document.getElementById('resultReason')
};

// Agent消息元素映射
const agentMessages = {
    'SequenceRecommender': document.getElementById('msg-SequenceRecommender'),
    'UserBehaviorAnalyzer': document.getElementById('msg-UserBehaviorAnalyzer'),
    'ProductAnalyzer': document.getElementById('msg-ProductAnalyzer'),
    'RecommendationSynthesizer': document.getElementById('msg-RecommendationSynthesizer')
};

// 检查服务状态
async function checkHealth() {
    elements.statusSection.style.display = 'block';
    elements.statusContent.innerHTML = '<span class="loading"></span> 检查服务状态中...';
    
    try {
        const response = await fetch(`${API_BASE_URL}/recommend/health`);
        const data = await response.json();
        
        let html = '<div>';
        html += `<p><strong>推荐系统状态:</strong> <span class="status-ok">✓ ${data.recommendation_system}</span></p>`;
        
        if (data.sasrec_service) {
            const sasrecStatus = data.sasrec_service.status || 'unknown';
            const modelLoaded = data.sasrec_service.model_loaded;
            const statusClass = modelLoaded ? 'status-ok' : 'status-error';
            const statusIcon = modelLoaded ? '✓' : '✗';
            
            html += `<p><strong>SASRec服务状态:</strong> <span class="${statusClass}">${statusIcon} ${sasrecStatus}</span></p>`;
            
            if (data.sasrec_service.dataset_info) {
                const info = data.sasrec_service.dataset_info;
                html += `<p><strong>数据集信息:</strong> 用户数: ${info.user_num}, 商品数: ${info.item_num}</p>`;
            }
        } else {
            html += `<p><strong>SASRec服务状态:</strong> <span class="status-error">✗ 不可用</span></p>`;
        }
        html += '</div>';
        
        elements.statusContent.innerHTML = html;
    } catch (error) {
        elements.statusContent.innerHTML = `<span class="status-error">✗ 无法连接到服务: ${error.message}</span>`;
    }
}

// 更新进度条
function updateProgress(percent, text) {
    elements.progressFill.style.width = `${percent}%`;
    elements.progressText.textContent = text;
}

// 更新Agent消息
function updateAgentMessage(agentKey, message) {
    if (agentMessages[agentKey]) {
        // 使用marked解析Markdown
        if (typeof marked !== 'undefined') {
            agentMessages[agentKey].innerHTML = marked.parse(message);
        } else {
            agentMessages[agentKey].textContent = message;
        }
    }
}

// 重置所有Agent消息
function resetAgentMessages() {
    Object.values(agentMessages).forEach(el => {
        el.innerHTML = '等待分析...';
    });
}

// 显示结果
function showResult(recommendation) {
    elements.resultSection.style.display = 'block';
    elements.resultProductId.textContent = recommendation.recommended_product || '-';
    elements.resultProductTitle.textContent = recommendation.product_title || '-';
    
    const confidence = recommendation.confidence;
    if (confidence !== undefined) {
        const percent = (confidence * 100).toFixed(1);
        elements.resultConfidence.textContent = `${percent}%`;
    } else {
        elements.resultConfidence.textContent = '-';
    }
    
    elements.resultReason.textContent = recommendation.recommendation_reason || '-';
}

// 开始推荐
async function startRecommend() {
    // 解析输入
    const itemSequenceStr = elements.itemSequence.value.trim();
    if (!itemSequenceStr) {
        alert('请输入商品ID序列');
        return;
    }
    
    const itemSequence = itemSequenceStr.split(',').map(s => s.trim()).filter(s => s);
    if (itemSequence.length === 0) {
        alert('请输入有效的商品ID序列');
        return;
    }
    
    const topK = parseInt(elements.topK.value) || 5;
    
    // 准备UI
    elements.startRecommend.disabled = true;
    elements.startRecommend.innerHTML = '推荐中... <span class="loading"></span>';
    elements.progressSection.style.display = 'block';
    elements.chatSection.style.display = 'block';
    elements.resultSection.style.display = 'none';
    
    resetAgentMessages();
    updateProgress(0, '准备中...');
    
    // 模拟进度
    let progress = 0;
    const progressInterval = setInterval(() => {
        if (progress < 90) {
            progress += Math.random() * 10;
            if (progress > 90) progress = 90;
            updateProgress(progress, `分析中... ${Math.round(progress)}%`);
        }
    }, 500);
    
    try {
        updateProgress(10, '发送推荐请求...');
        
        const response = await fetch(`${API_BASE_URL}/recommend`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                item_sequence: itemSequence,
                top_k: topK
            })
        });
        
        clearInterval(progressInterval);
        
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        
        const data = await response.json();
        
        if (data.success) {
            updateProgress(95, '处理结果...');
            
            // 更新Agent对话
            const conversation = data.conversation || {};
            for (const [agentKey, message] of Object.entries(conversation)) {
                updateAgentMessage(agentKey, message);
            }
            
            // 显示最终结果
            updateProgress(100, '推荐完成！');
            showResult(data.recommendation);
            
        } else {
            throw new Error(data.message || '推荐失败');
        }
        
    } catch (error) {
        clearInterval(progressInterval);
        updateProgress(0, `错误: ${error.message}`);
        alert(`推荐失败: ${error.message}`);
        
        // 显示错误信息到Agent对话
        Object.values(agentMessages).forEach(el => {
            el.innerHTML = `<span style="color: #dc3545;">请求失败: ${error.message}</span>`;
        });
        
    } finally {
        elements.startRecommend.disabled = false;
        elements.startRecommend.innerHTML = '开始推荐';
    }
}

// 绑定事件
elements.checkHealth.addEventListener('click', checkHealth);
elements.startRecommend.addEventListener('click', startRecommend);

// 页面加载时自动检查服务状态
document.addEventListener('DOMContentLoaded', () => {
    checkHealth();
});
