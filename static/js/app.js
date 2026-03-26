class ArbitrageDashboard {
    constructor() {
        this.ws = null;
        this.opportunities = [];
        this.currentTrade = null;
        this.selectedOpportunityIndex = 0;
        this.wsConnected = false;
        this.activeSymbolsCount = null;
        this.autoTrading = null;
        this.broker = {
            defaultTradeNotionalUsdt: 12.5,
            futuresMarginType: 'ISOLATED',
            futuresLeverage: 2,
            marginShortEnabled: false
        };
        this.connectWebSocket();
        this.loadInitialData();
    }

    connectWebSocket() {
        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const wsUrl = `${protocol}//${window.location.host}/ws/dashboard`;

        this.ws = new WebSocket(wsUrl);

        this.ws.onopen = () => {
            this.updateConnectionStatus(true);
            this.wsConnected = true;
        };

        this.ws.onmessage = (event) => {
            try {
                const data = JSON.parse(event.data);
                this.handleWebSocketMessage(data);
            } catch (error) {
                console.error('Failed to parse WebSocket message:', error);
            }
        };

        this.ws.onclose = () => {
            this.updateConnectionStatus(false);
            this.wsConnected = false;
            setTimeout(() => this.connectWebSocket(), 3000);
        };

        this.ws.onerror = () => {
            this.updateConnectionStatus(false);
        };
    }

    async loadInitialData() {
        try {
            const response = await fetch('/api/opportunities');
            const result = await response.json();

            if (result.success) {
                this.setOpportunities(result.data);
                this.autoTrading = result.auto_trading || null;
                if (Number.isFinite(result.active_symbols)) {
                    this.activeSymbolsCount = result.active_symbols;
                }
                this.updateBrokerSettings(result);
                this.updateDashboard();
                this.updateStats();
                this.updateAutoTradingPanel();
                this.setLastUpdate(result.timestamp || new Date().toISOString());
            } else {
                this.showFallbackData();
            }
        } catch (error) {
            console.error('Failed to fetch opportunities:', error);
            this.showFallbackData();
        }
    }

    showFallbackData() {
        this.setOpportunities([
            {
                symbol: 'BTCUSDT',
                equity_bid: 74200.15,
                equity_ask: 74200.65,
                futures_bid: 74220.10,
                futures_ask: 74221.55,
                spread: 19.95,
                spread_percent: 0.027,
                execution_spread_percent: 0.026,
                funding_3d_percent: 0.182,
                apr_3d_percent: 22.15,
                next_funding_rate_percent: 0.012,
                opportunity_type: 'BUY_EQUITY_SELL_FUTURES',
                funding_side: 'SHORT_FUTURES_RECEIVES',
                action: 'Buy Spot / Sell Perp',
                can_place_order: true,
                timestamp: new Date().toISOString()
            },
            {
                symbol: 'ETHUSDT',
                equity_bid: 3920.1,
                equity_ask: 3920.6,
                futures_bid: 3928.2,
                futures_ask: 3929.2,
                spread: 8.35,
                spread_percent: 0.213,
                execution_spread_percent: 0.194,
                funding_3d_percent: 0.091,
                apr_3d_percent: 11.07,
                next_funding_rate_percent: 0.008,
                opportunity_type: 'BUY_EQUITY_SELL_FUTURES',
                funding_side: 'SHORT_FUTURES_RECEIVES',
                action: 'Buy Spot / Sell Perp',
                can_place_order: true,
                timestamp: new Date().toISOString()
            }
        ]);

        this.updateDashboard();
        this.updateStats();
        this.updateAutoTradingPanel();
        this.setLastUpdate(new Date().toISOString());
    }

    handleWebSocketMessage(data) {
        if (data.type === 'initial' || data.type === 'update') {
            this.setOpportunities(data.opportunities);
            this.autoTrading = data.auto_trading || this.autoTrading;
            if (Number.isFinite(data.active_symbols)) {
                this.activeSymbolsCount = data.active_symbols;
            }
            this.updateBrokerSettings(data);
            this.updateDashboard();
            this.updateStats();
            this.updateAutoTradingPanel();
            this.setLastUpdate(data.timestamp || new Date().toISOString());
        }
    }

    updateBrokerSettings(payload) {
        const broker = payload && typeof payload === 'object' ? payload.broker : null;
        if (!broker || typeof broker !== 'object') {
            return;
        }

        const notional = Number(broker.default_trade_notional_usdt);
        if (Number.isFinite(notional) && notional > 0) {
            this.broker.defaultTradeNotionalUsdt = notional;
        }

        const marginType = String(broker.futures_margin_type || '').toUpperCase();
        if (marginType) {
            this.broker.futuresMarginType = marginType;
        }

        const leverage = Number(broker.futures_leverage);
        if (Number.isFinite(leverage) && leverage > 0) {
            this.broker.futuresLeverage = leverage;
        }

        this.broker.marginShortEnabled = Boolean(broker.margin_short_enabled);
    }

    getTradeDefaultNotional() {
        return Number(this.broker.defaultTradeNotionalUsdt || 12.5);
    }

    setOpportunities(opportunities) {
        const previousSymbol = this.opportunities[this.selectedOpportunityIndex]?.symbol || null;
        this.opportunities = this.sortOpportunitiesByApr(opportunities);

        if (previousSymbol) {
            const selectedIndex = this.opportunities.findIndex((item) => item.symbol === previousSymbol);
            if (selectedIndex >= 0) {
                this.selectedOpportunityIndex = selectedIndex;
                return;
            }
        }

        this.normalizeSelectedIndex();
    }

    sortOpportunitiesByApr(opportunities) {
        return [...(Array.isArray(opportunities) ? opportunities : [])].sort((a, b) => {
            const aprDiff = Number(b.apr_3d_percent || 0) - Number(a.apr_3d_percent || 0);
            if (aprDiff !== 0) {
                return aprDiff;
            }

            const executionSpreadDiff = Number(b.execution_spread_percent || 0) - Number(a.execution_spread_percent || 0);
            if (executionSpreadDiff !== 0) {
                return executionSpreadDiff;
            }

            const spreadDiff = Number(b.spread_percent || 0) - Number(a.spread_percent || 0);
            if (spreadDiff !== 0) {
                return spreadDiff;
            }

            return String(a.symbol || '').localeCompare(String(b.symbol || ''));
        });
    }

    normalizeSelectedIndex() {
        if (!this.opportunities.length) {
            this.selectedOpportunityIndex = 0;
            return;
        }
        if (this.selectedOpportunityIndex >= this.opportunities.length) {
            this.selectedOpportunityIndex = 0;
        }
    }

    setLastUpdate(timestamp) {
        const element = document.getElementById('last-update');
        if (!element) return;
        const date = new Date(timestamp);
        element.textContent = Number.isNaN(date.getTime()) ? '--:--:--' : date.toLocaleTimeString();
    }

    updateDashboard() {
        const ranked = this.buildRankedLists();
        this.renderRankingTable('hot-coins-table', ranked.hot, 'apr');
        this.renderRankingTable('gainers-table', ranked.gainers, 'spread');
        this.renderRankingTable('losers-table', ranked.losers, 'spread');
        this.renderOpportunitiesTable();
        this.renderSelectedOpportunity();
    }

    buildRankedLists() {
        const hot = [...this.opportunities].sort((a, b) => {
            const scoreA = Math.abs(Number(a.apr_3d_percent || 0)) + Math.abs(Number(a.spread_percent || 0)) * 10;
            const scoreB = Math.abs(Number(b.apr_3d_percent || 0)) + Math.abs(Number(b.spread_percent || 0)) * 10;
            return scoreB - scoreA;
        });

        const gainers = [...this.opportunities].sort((a, b) => Number(b.spread_percent || 0) - Number(a.spread_percent || 0));
        const losers = [...this.opportunities].sort((a, b) => Number(a.spread_percent || 0) - Number(b.spread_percent || 0));

        return {
            hot: this.fillPlaceholders(hot.slice(0, 10)),
            gainers: this.fillPlaceholders(gainers.slice(0, 10)),
            losers: this.fillPlaceholders(losers.slice(0, 10))
        };
    }

    fillPlaceholders(rows) {
        const filled = [...rows];
        while (filled.length < 10) {
            filled.push({ placeholder: true });
        }
        return filled;
    }

    renderRankingTable(targetId, items, metricMode) {
        const tableBody = document.getElementById(targetId);
        if (!tableBody) return;

        tableBody.innerHTML = items.map((item, index) => {
            if (item.placeholder) {
                return `
                    <tr class="placeholder-row">
                        <td class="rank-index">${index + 1}</td>
                        <td><div class="coin-cell"><div class="coin-icon">-</div><div><div class="coin-name">Waiting</div><div class="coin-meta">Live scan in progress</div></div></div></td>
                        <td class="price-text">--</td>
                        <td>--</td>
                    </tr>
                `;
            }

            const actualIndex = this.opportunities.indexOf(item);
            const symbol = this.escapeHtml(item.symbol || '');
            const metric = metricMode === 'apr'
                ? `${this.formatPercent(item.apr_3d_percent)}%`
                : `${this.formatPercent(item.spread_percent)}%`;
            const metricClass = Number(metricMode === 'apr' ? item.apr_3d_percent : item.spread_percent) >= 0
                ? 'change-positive'
                : 'change-negative';
            const price = this.formatDisplayPrice(this.primaryDisplayPrice(item));
            const meta = metricMode === 'apr'
                ? `${this.escapeHtml(item.funding_side || this.getOpportunityLabel(item.opportunity_type))}`
                : `${this.escapeHtml(this.getOpportunityLabel(item.opportunity_type))}`;

            return `
                <tr onclick="dashboard.selectOpportunity(${actualIndex})" style="cursor: pointer;">
                    <td class="rank-index">${index + 1}</td>
                    <td>
                        <div class="coin-cell">
                            <div class="coin-icon">${this.symbolInitials(symbol)}</div>
                            <div>
                                <div class="coin-name">${symbol}</div>
                                <div class="coin-meta">${meta}</div>
                            </div>
                        </div>
                    </td>
                    <td class="price-text">${price}</td>
                    <td class="${metricClass}">${metric}</td>
                </tr>
            `;
        }).join('');
    }

    renderOpportunitiesTable() {
        const tableBody = document.getElementById('opportunities-table');
        if (!tableBody) return;

        if (!this.opportunities.length) {
            tableBody.innerHTML = `
                <tr>
                    <td colspan="10" class="has-text-centered py-5">
                        <p>No arbitrage opportunities available right now.</p>
                    </td>
                </tr>
            `;
            return;
        }

        tableBody.innerHTML = this.opportunities.map((opp, index) => {
            const spreadPercent = Number(opp.spread_percent || 0);
            const spreadClass = spreadPercent >= 0 ? 'spread-positive' : 'spread-negative';
            const canTrade = opp.can_place_order !== false;
            const tradeLabel = canTrade ? 'Trade' : 'Blocked';
            const actionText = this.escapeHtml(opp.action || this.getOpportunityLabel(opp.opportunity_type));
            const blockedReason = opp.can_place_order === false
                ? ` title="${this.escapeHtml(opp.trade_blocked_reason || 'Trading is blocked for this symbol.')}" `
                : '';

            return `
                <tr class="opportunity-row" onclick="dashboard.selectOpportunity(${index})" style="cursor: pointer;">
                    <td><strong>${this.escapeHtml(opp.symbol || '')}</strong></td>
                    <td>${this.formatDisplayPrice(opp.equity_bid)} / ${this.formatDisplayPrice(opp.equity_ask)}</td>
                    <td>${this.formatDisplayPrice(opp.futures_bid)} / ${this.formatDisplayPrice(opp.futures_ask)}</td>
                    <td class="${spreadClass}">${this.formatDisplayPrice(opp.spread)}</td>
                    <td class="${spreadClass}">${this.formatPercent(opp.spread_percent)}%</td>
                    <td>${this.formatPercent(this.calculateTargetExitSpread(spreadPercent))}%</td>
                    <td>${this.formatPercent(opp.funding_3d_percent)}%</td>
                    <td>${this.formatPercent(opp.apr_3d_percent)}%</td>
                    <td${blockedReason}>${actionText}</td>
                    <td>
                        <button
                            class="button is-small ${canTrade ? 'is-success' : 'is-light'}"
                            onclick="event.stopPropagation(); dashboard.initiateArbitrageTrade(${index})"
                            ${canTrade ? '' : 'disabled'}
                        >
                            ${tradeLabel}
                        </button>
                    </td>
                </tr>
            `;
        }).join('');
    }

    renderSelectedOpportunity() {
        const panel = document.getElementById('selected-opportunity-panel');
        const tradeBtn = document.getElementById('selected-trade-btn');
        if (!panel || !tradeBtn) return;

        const opp = this.opportunities[this.selectedOpportunityIndex];
        if (!opp) {
            tradeBtn.disabled = true;
            panel.innerHTML = `
                <div class="detail-symbol">
                    <div>
                        <h3>No selection</h3>
                        <p>Waiting for live arbitrage opportunities</p>
                    </div>
                    <div class="detail-tag">Standby</div>
                </div>
                <div class="detail-data-grid">
                    <div class="detail-data"><div class="label">Spot Bid / Ask</div><div class="value">-</div></div>
                    <div class="detail-data"><div class="label">Perp Bid / Ask</div><div class="value">-</div></div>
                    <div class="detail-data"><div class="label">Spread</div><div class="value">-</div></div>
                    <div class="detail-data"><div class="label">3D APR</div><div class="value">-</div></div>
                </div>
                <div class="detail-notes">As live opportunities stream in, this panel highlights the best setup with funding direction, prices, exit target, and order status.</div>
                <div class="detail-actions">
                    <button class="action-btn primary" id="selected-trade-btn" onclick="dashboard.tradeSelectedOpportunity()" disabled>Place Order</button>
                    <button class="action-btn secondary" id="selected-refresh-btn" onclick="dashboard.focusBestOpportunity()">Focus Best</button>
                </div>
            `;
            return;
        }

        const spreadValue = Number(opp.spread_percent || 0);
        const spreadClass = spreadValue >= 0 ? 'change-positive' : 'change-negative';
        const canTrade = opp.can_place_order !== false;
        const exitSpread = this.calculateTargetExitSpread(spreadValue);

        panel.innerHTML = `
            <div class="detail-symbol">
                <div>
                    <h3>${this.escapeHtml(opp.symbol || '')}</h3>
                    <p>${this.escapeHtml(opp.action || this.getOpportunityLabel(opp.opportunity_type))}</p>
                </div>
                <div class="detail-tag ${spreadClass}">${this.formatPercent(spreadValue)}%</div>
            </div>
            <div class="detail-data-grid">
                <div class="detail-data"><div class="label">Spot Bid / Ask</div><div class="value">${this.formatDisplayPrice(opp.equity_bid)} / ${this.formatDisplayPrice(opp.equity_ask)}</div></div>
                <div class="detail-data"><div class="label">Perp Bid / Ask</div><div class="value">${this.formatDisplayPrice(opp.futures_bid)} / ${this.formatDisplayPrice(opp.futures_ask)}</div></div>
                <div class="detail-data"><div class="label">Current Spread</div><div class="value ${spreadClass}">${this.formatDisplayPrice(opp.spread)} (${this.formatPercent(opp.spread_percent)}%)</div></div>
                <div class="detail-data"><div class="label">Execution Spread</div><div class="value">${this.formatPercent(opp.execution_spread_percent)}%</div></div>
                <div class="detail-data"><div class="label">3D Funding / APR</div><div class="value">${this.formatPercent(opp.funding_3d_percent)}% / ${this.formatPercent(opp.apr_3d_percent)}%</div></div>
                <div class="detail-data"><div class="label">Target Exit</div><div class="value">${this.formatPercent(exitSpread)}%</div></div>
            </div>
            <div class="detail-notes">
                <strong>${this.escapeHtml(opp.funding_side || 'Funding side unavailable')}</strong><br>
                ${opp.can_place_order === false
                    ? this.escapeHtml(opp.trade_blocked_reason || 'Trading is blocked for this symbol.')
                    : 'Order path is available. This paired trade will send one spot leg and one perpetual leg using the current displayed prices.'}
            </div>
            <div class="detail-actions">
                <button class="action-btn primary" id="selected-trade-btn" onclick="dashboard.tradeSelectedOpportunity()" ${canTrade ? '' : 'disabled'}>${canTrade ? 'Place Order' : 'Trade Blocked'}</button>
                <button class="action-btn secondary" id="selected-refresh-btn" onclick="dashboard.focusBestOpportunity()">Focus Best</button>
            </div>
        `;
    }

    selectOpportunity(index) {
        if (!Number.isInteger(index) || index < 0 || index >= this.opportunities.length) {
            return;
        }
        this.selectedOpportunityIndex = index;
        this.renderSelectedOpportunity();
    }

    focusBestOpportunity() {
        if (!this.opportunities.length) return;
        const bestIndex = this.opportunities.reduce((best, current, index, list) => {
            const bestScore = Math.abs(Number(list[best].apr_3d_percent || 0)) + Math.abs(Number(list[best].spread_percent || 0)) * 10;
            const currentScore = Math.abs(Number(current.apr_3d_percent || 0)) + Math.abs(Number(current.spread_percent || 0)) * 10;
            return currentScore > bestScore ? index : best;
        }, 0);
        this.selectedOpportunityIndex = bestIndex;
        this.renderSelectedOpportunity();
    }

    tradeSelectedOpportunity() {
        this.initiateArbitrageTrade(this.selectedOpportunityIndex);
    }

    calculateTargetExitSpread(entrySpreadPercent) {
        const state = this.autoTrading || {};
        const config = state.config || {};
        const estimatedCost = Number(config.estimated_cost_percent || 0);
        const minExitProfit = Number(config.min_exit_profit_percent || 0);
        const configuredTarget = Number(config.exit_spread_target_percent ?? -0.1);
        const noLossTarget = Math.max(0, estimatedCost + minExitProfit - Number(entrySpreadPercent || 0));
        return Math.max(configuredTarget, noLossTarget);
    }

    updateStats() {
        const totalElement = document.getElementById('total-opportunities');
        const activeElement = document.getElementById('active-symbols');
        const bestElement = document.getElementById('best-spread');
        if (!totalElement || !activeElement || !bestElement) return;

        totalElement.textContent = this.opportunities.length;
        const activeSymbols = Number.isFinite(this.activeSymbolsCount)
            ? this.activeSymbolsCount
            : new Set(this.opportunities.map((item) => item.symbol)).size;
        activeElement.textContent = activeSymbols;

        if (this.opportunities.length) {
            const bestSpread = Math.max(...this.opportunities.map((item) => Number(item.spread_percent || 0)));
            bestElement.textContent = `${this.formatPercent(bestSpread)}%`;
            bestElement.className = `stat-value ${bestSpread >= 0 ? 'change-positive' : 'change-negative'}`;
        } else {
            bestElement.textContent = '0.000%';
            bestElement.className = 'stat-value';
        }
    }

    updateAutoTradingPanel() {
        const state = this.autoTrading || {};
        const config = state.config || {};
        const enabled = Boolean(state.enabled);
        const lastAction = state.last_actions && state.last_actions.length ? state.last_actions[0] : null;
        const lastError = state.last_errors && state.last_errors.length ? state.last_errors[0] : null;

        const mode = document.getElementById('auto-trading-mode');
        const openPositions = document.getElementById('auto-open-positions');
        const lastActionEl = document.getElementById('auto-last-action');
        const lastErrorEl = document.getElementById('auto-last-error');
        const rankWindowEl = document.getElementById('auto-rank-window');
        const summaryEl = document.getElementById('auto-trading-summary');
        const detailEl = document.getElementById('auto-detail-box');
        const startBtn = document.getElementById('auto-start-btn');
        const stopBtn = document.getElementById('auto-stop-btn');

        if (!mode) return;

        mode.className = enabled ? 'status-chip enabled' : 'status-chip';
        mode.textContent = enabled ? 'Enabled' : 'Disabled';
        openPositions.textContent = state.open_positions || 0;
        lastActionEl.textContent = lastAction ? `${lastAction.symbol} ${lastAction.action}` : '-';
        lastErrorEl.textContent = lastError ? `${lastError.symbol} ${lastError.code}` : '-';

        const topCount = Number.isFinite(config.top_candidate_count) ? config.top_candidate_count : 0;
        const entry = Number.isFinite(config.entry_threshold_percent) ? Number(config.entry_threshold_percent).toFixed(3) : '0.000';
        const exitTarget = Number.isFinite(config.exit_spread_target_percent) ? Number(config.exit_spread_target_percent).toFixed(3) : '-0.100';
        const persistence = Number.isFinite(config.persistence_scans) ? config.persistence_scans : 1;
        const cost = Number.isFinite(config.estimated_cost_percent) ? Number(config.estimated_cost_percent).toFixed(3) : '0.000';

        rankWindowEl.textContent = `Top ${topCount}`;
        summaryEl.textContent = `Top ${topCount} ranked setups, entry >= ${entry}%, estimated cost ${cost}%, managed exits at ${exitTarget}% or better.`;
        detailEl.textContent = `Persistence filter: ${persistence} scan${persistence === 1 ? '' : 's'}. Open positions cap: ${state.config?.max_open_positions ?? 0}. The auto trader prioritizes high APR carry with valid execution paths.`;

        startBtn.disabled = enabled;
        stopBtn.disabled = !enabled;
    }

    async toggleAutoTrading(enable) {
        const endpoint = enable ? '/api/auto-trading/start' : '/api/auto-trading/stop';
        try {
            const response = await fetch(endpoint, { method: 'POST' });
            const result = await response.json();
            this.autoTrading = result.auto_trading || this.autoTrading;
            this.updateAutoTradingPanel();
            this.showNotification(`Auto trading ${enable ? 'started' : 'stopped'}`, 'is-info');
        } catch (error) {
            console.error('Failed to toggle auto trading:', error);
            this.showNotification('Failed to update auto-trading state', 'is-danger');
        }
    }

    updateConnectionStatus(connected) {
        const statusElement = document.getElementById('ws-status');
        const textElement = document.getElementById('connection-text');
        if (!statusElement || !textElement) return;

        statusElement.className = `ws-dot ${connected ? 'connected' : ''}`.trim();
        textElement.textContent = connected ? 'Connected (Live)' : 'Disconnected - Polling';
    }

    getOpportunityLabel(type) {
        if (!type) return 'Arbitrage Opportunity';
        return type.replace(/_/g, ' ');
    }

    primaryDisplayPrice(opp) {
        if (!opp) return 0;
        const type = opp.opportunity_type;
        if (type === 'SELL_EQUITY_BUY_FUTURES') {
            return Number(opp.equity_bid || opp.futures_ask || 0);
        }
        return Number(opp.equity_ask || opp.futures_bid || 0);
    }

    formatDisplayPrice(value) {
        const num = Number(value);
        if (!Number.isFinite(num)) return '--';
        if (Math.abs(num) >= 1000) return num.toLocaleString(undefined, { maximumFractionDigits: 2 });
        if (Math.abs(num) >= 1) return num.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 4 });
        return num.toFixed(6);
    }

    formatPercent(value) {
        const num = Number(value);
        if (!Number.isFinite(num)) return '0.000';
        return num > 0 ? `+${num.toFixed(3)}` : num.toFixed(3);
    }

    symbolInitials(symbol) {
        const cleaned = String(symbol || '').replace('USDT', '').replace(/[^A-Z0-9]/gi, '');
        return cleaned.slice(0, 2).toUpperCase() || '--';
    }

    escapeHtml(value) {
        return String(value)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#39;');
    }

    initiateArbitrageTrade(index) {
        const opp = this.opportunities[index];
        if (!opp) return;
        if (opp.can_place_order === false) {
            this.showNotification(`Trade Blocked: ${opp.trade_blocked_reason || 'Spot inventory required'}`, 'is-warning');
            return;
        }

        const isBuyEquity = opp.opportunity_type !== 'SELL_EQUITY_BUY_FUTURES';
        const legs = isBuyEquity
            ? [
                { side: 'BUY', instrument: 'EQUITY', price: opp.equity_ask || 0 },
                { side: 'SELL', instrument: 'FUTURES', price: opp.futures_bid || 0 }
            ]
            : [
                { side: 'SELL', instrument: 'EQUITY', price: opp.equity_bid || 0 },
                { side: 'BUY', instrument: 'FUTURES', price: opp.futures_ask || 0 }
            ];

        this.currentTrade = {
            symbol: opp.symbol,
            opportunityType: opp.opportunity_type,
            spreadPercent: opp.spread_percent,
            equityPrice: opp.equity_ask || opp.equity_bid || 0,
            futuresPrice: opp.futures_bid || opp.futures_ask || 0,
            legs
        };

        const details = document.getElementById('trade-details');
        details.innerHTML = `
            <div class="detail-notes" style="margin-top:0;">
                <strong>${this.escapeHtml(opp.symbol)}</strong><br>
                ${this.escapeHtml(this.getOpportunityLabel(opp.opportunity_type))}<br><br>
                Leg 1: ${legs[0].side} ${legs[0].instrument === 'EQUITY' ? 'SPOT' : 'PERP'} @ ${this.formatDisplayPrice(legs[0].price)}<br>
                Leg 2: ${legs[1].side} ${legs[1].instrument === 'EQUITY' ? 'SPOT' : 'PERP'} @ ${this.formatDisplayPrice(legs[1].price)}<br><br>
                Spread: <span class="${Number(opp.spread_percent || 0) >= 0 ? 'change-positive' : 'change-negative'}">${this.formatPercent(opp.spread_percent)}%</span><br>
                3D APR: ${this.formatPercent(opp.apr_3d_percent)}%
            </div>
        `;

        const tradeInput = document.getElementById('trade-quantity');
        if (tradeInput) {
            tradeInput.value = String(this.getTradeDefaultNotional());
        }

        const tradeNote = document.getElementById('trade-mode-note');
        if (tradeNote) {
            const marginType = this.escapeHtml(this.broker.futuresMarginType || 'ISOLATED');
            const leverage = Number(this.broker.futuresLeverage || 0);
            const leverageText = Number.isFinite(leverage) && leverage > 0 ? ` at ${leverage}x leverage` : '';
            tradeNote.innerHTML = `
                <i class="fas fa-info-circle"></i>
                Real mode: trade size is <strong>USDT per leg</strong>. This will send a real Binance spot order and a real Binance perpetual order. Futures orders will request <strong>${marginType}</strong> margin${leverageText}.
            `;
        }

        document.getElementById('trade-modal').classList.remove('is-hidden');
    }

    async executeTrade(quantity) {
        if (!this.currentTrade) return;

        const tradeNotional = parseFloat(quantity);
        if (!Number.isFinite(tradeNotional) || tradeNotional <= 0) {
            this.showNotification('Trade Failed: Trade size must be a positive USDT amount', 'is-danger');
            return;
        }

        try {
            const response = await fetch('/api/trade/pair', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    symbol: this.currentTrade.symbol,
                    opportunity_type: this.currentTrade.opportunityType,
                    equity_price: this.currentTrade.equityPrice,
                    futures_price: this.currentTrade.futuresPrice,
                    trade_notional_usdt: tradeNotional,
                    action: 'ENTRY'
                })
            }).then((r) => r.json());

            if (response.status !== 'EXECUTED') {
                this.showNotification(`Trade Failed: ${response.message || 'Unknown error'}`, 'is-danger');
            } else {
                const legResults = Array.isArray(response.leg_results) ? response.leg_results : [];
                const eqResult = legResults.find((r) => r.details?.instrument === 'EQUITY');
                const futResult = legResults.find((r) => r.details?.instrument === 'FUTURES');
                const eqId = eqResult?.trade_id || 'NA';
                const futId = futResult?.trade_id || 'NA';
                this.showNotification(
                    `Trade Executed: ${this.currentTrade.symbol} ${tradeNotional} USDT per leg, SPOT order ${eqId}, PERP order ${futId}`,
                    'is-success'
                );
            }
        } catch (error) {
            console.error('Trade execution failed:', error);
            this.showNotification('Trade execution failed. Please try again.', 'is-danger');
        }

        closeTradeModal();
    }

    showNotification(message, type = 'is-info') {
        const existing = document.querySelector('.notification-toast');
        if (existing) existing.remove();

        const notification = document.createElement('div');
        notification.className = `notification-toast ${type}`;
        notification.innerHTML = `
            <button class="toast-close" onclick="this.parentElement.remove()"><i class="fas fa-times"></i></button>
            ${this.escapeHtml(message)}
        `;
        document.body.appendChild(notification);

        setTimeout(() => {
            if (notification.parentElement) {
                notification.remove();
            }
        }, 5000);
    }
}

function closeTradeModal() {
    document.getElementById('trade-modal').classList.add('is-hidden');
}

function confirmTrade() {
    const quantity = document.getElementById('trade-quantity').value;
    if (quantity && dashboard) {
        dashboard.executeTrade(quantity);
    }
}

let dashboard;
document.addEventListener('DOMContentLoaded', () => {
    dashboard = new ArbitrageDashboard();

    setInterval(() => {
        if (!dashboard.wsConnected) {
            fetch('/api/opportunities')
                .then((response) => response.json())
                .then((result) => {
                    if (result.success) {
                        dashboard.setOpportunities(result.data);
                        dashboard.autoTrading = result.auto_trading || dashboard.autoTrading;
                        if (Number.isFinite(result.active_symbols)) {
                            dashboard.activeSymbolsCount = result.active_symbols;
                        }
                        dashboard.updateDashboard();
                        dashboard.updateStats();
                        dashboard.updateAutoTradingPanel();
                        dashboard.setLastUpdate(result.timestamp || new Date().toISOString());
                    }
                })
                .catch(() => {
                    console.log('Polling failed, WebSocket may be active');
                });
        }
    }, 10000);
});
