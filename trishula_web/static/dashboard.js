        let stateData = null;
        let selectedFunnelSteps = [];
        let allTopEventsList = [];
        let currentSessionResults = [];
        const expandedSessionRows = new Set();
        let activeTab = 'overview';
        const loadedTabs = new Set();
        const tabLoadPromises = new Map();
        let loadingSequence = 0;

        function escapeHtml(value) {
            const node = document.createElement('span');
            node.textContent = String(value ?? '');
            return node.innerHTML;
        }

        function startLoading({
            title = 'Working on your analysis',
            stage = 'Preparing…',
            controls = [],
            showDelayMs = 250
        } = {}) {
            const operationId = ++loadingSequence;
            const overlay = document.getElementById('loadingOverlay');
            const titleElement = document.getElementById('loadingTitle');
            const stageElement = document.getElementById('loadingStage');
            const elapsedElement = document.getElementById('loadingElapsed');
            const progressTrack = document.getElementById('loadingProgressTrack');
            const progressBar = document.getElementById('loadingProgressBar');
            const progressLabel = document.getElementById('loadingProgressLabel');
            const longHint = document.getElementById('loadingLongHint');
            const startedAt = performance.now();
            let shownAt = null;
            let finished = false;
            const controlledElements = Array.from(controls).filter(Boolean);
            const previousDisabledStates = controlledElements.map(element => element.disabled);
            controlledElements.forEach(element => { element.disabled = true; });

            titleElement.textContent = title;
            stageElement.textContent = stage;
            elapsedElement.textContent = '';
            progressTrack.classList.add('indeterminate');
            progressBar.style.width = '';
            progressLabel.textContent = 'Working…';
            longHint.classList.remove('visible');
            overlay.classList.add('pending');

            const show = () => {
                if (finished || operationId !== loadingSequence) return;
                shownAt = performance.now();
                overlay.classList.remove('pending');
                overlay.classList.add('visible');
                overlay.setAttribute('aria-hidden', 'false');
            };
            const showTimer = window.setTimeout(show, showDelayMs);
            const elapsedTimer = window.setInterval(() => {
                if (operationId !== loadingSequence) return;
                const elapsedSeconds = Math.floor((performance.now() - startedAt) / 1000);
                elapsedElement.textContent = elapsedSeconds > 0 ? `${elapsedSeconds}s elapsed` : '';
                longHint.classList.toggle('visible', elapsedSeconds >= 10);
            }, 500);

            return {
                setStage(nextStage) {
                    if (operationId === loadingSequence) stageElement.textContent = nextStage;
                },
                setProgress(percent) {
                    if (operationId !== loadingSequence) return;
                    if (percent === null || percent === undefined) {
                        progressTrack.classList.add('indeterminate');
                        progressBar.style.width = '';
                        progressLabel.textContent = 'Working…';
                        return;
                    }
                    const bounded = Math.max(0, Math.min(100, Math.round(percent)));
                    progressTrack.classList.remove('indeterminate');
                    progressBar.style.width = `${bounded}%`;
                    progressLabel.textContent = `${bounded}%`;
                },
                async finish() {
                    if (finished) return;
                    finished = true;
                    window.clearTimeout(showTimer);
                    window.clearInterval(elapsedTimer);
                    previousDisabledStates.forEach((wasDisabled, index) => {
                        controlledElements[index].disabled = wasDisabled;
                    });
                    if (operationId !== loadingSequence) return;
                    const visibleFor = shownAt ? performance.now() - shownAt : 0;
                    if (shownAt && visibleFor < 350) {
                        await new Promise(resolve => window.setTimeout(resolve, 350 - visibleFor));
                    }
                    overlay.classList.remove('pending');
                    overlay.classList.remove('visible');
                    overlay.setAttribute('aria-hidden', 'true');
                }
            };
        }

        window.addEventListener('DOMContentLoaded', () => {
            fetchState();
            document.querySelectorAll('[data-tab]').forEach(button => {
                button.addEventListener('click', () => switchTab(button.dataset.tab));
            });
            document.getElementById('openFileButton').addEventListener('click', toggleFileLoader);
            document.getElementById('dedupeSelect').addEventListener('change', onDedupeChange);
            document.getElementById('printButton').addEventListener('click', exportSelectedTabToPdf);
            document.getElementById('browserFileButton').addEventListener(
                'click', () => document.getElementById('browserFileInput').click()
            );
            document.getElementById('browserFileInput').addEventListener(
                'change', event => handleBrowserFileUpload(event.target.files)
            );
            document.querySelectorAll('.funnel-preset').forEach(button => {
                button.addEventListener('click', () => applyFunnelPreset(button.dataset.preset));
            });
            document.getElementById('clearFunnelButton').addEventListener('click', clearFunnel);
            document.getElementById('calculateFunnelButton').addEventListener('click', loadFunnel);
            document.getElementById('addEventSelect').addEventListener(
                'change', event => addStepToFunnel(event.target.value)
            );
            document.getElementById('searchButton').addEventListener('click', runSearch);
            document.getElementById('collapseAllSessionsButton').addEventListener('click', () => {
                expandedSessionRows.clear();
                renderSessionResults();
            });
            document.getElementById('unloadButton').addEventListener('click', unloadDataset);
        });

        async function fetchState() {
            try {
                const res = await fetch('/api/state');
                stateData = await res.json();
                if (stateData.loaded) {
                    document.getElementById('loaderCard').style.display = 'none';
                    document.getElementById('openFileButton').setAttribute('aria-expanded', 'false');
                    document.getElementById('datasetBanner').style.display = 'flex';
                    document.getElementById('activeFileName').innerText = stateData.parquet_file;
                    document.getElementById('activeFileSize').innerText = `(${stateData.file_size_mb} MB)`;
                    document.getElementById('duckdbStatus').textContent =
                        `⚡ DuckDB: ${stateData.duckdb_threads} threads · ${stateData.duckdb_memory_limit}`;
                    loadStorageStatus();
                    resetTabLoads();
                    await loadTabData(activeTab);
                } else {
                    document.getElementById('loaderCard').style.display = 'block';
                    document.getElementById('openFileButton').setAttribute('aria-expanded', 'true');
                    document.getElementById('datasetBanner').style.display = 'none';
                    resetTabLoads();
                }
            } catch (err) {
                console.error(err);
            }
        }

        async function loadStorageStatus() {
            const response = await fetch('/api/storage');
            if (!response.ok) return;
            const storage = await response.json();
            document.getElementById('storageStatus').textContent =
                `${storage.disk_free_gb} GB free · ${storage.managed_dataset_mb} MB managed`;
        }

        async function unloadDataset() {
            if (!confirm('Unload this dataset? Uploaded managed files will be deleted.')) return;
            const response = await fetch('/api/unload', {method: 'POST'});
            if (response.ok) {
                stateData = {loaded: false};
                selectedFunnelSteps = [];
                resetTabLoads();
                await fetchState();
            }
        }

        function collectPdfBlocks(panel) {
            const blocks = [];
            panel.querySelectorAll('h2, h3, p, li, pre, table, .kpi-card, [role="status"]').forEach(element => {
                if (window.getComputedStyle(element).display === 'none') return;
                if (element.matches('h2, h3')) {
                    blocks.push({type: 'heading', text: element.textContent.trim()});
                } else if (element.matches('li')) {
                    blocks.push({type: 'list-item', text: element.textContent.trim()});
                } else if (element.matches('pre')) {
                    blocks.push({type: 'code', text: element.textContent.trim()});
                } else if (element.matches('table')) {
                    const rows = Array.from(element.querySelectorAll('tr'))
                        .filter(row => window.getComputedStyle(row).display !== 'none')
                        .map(row => Array.from(row.querySelectorAll('th, td'))
                            .map(cell => cell.textContent.trim()))
                        .filter(row => row.length > 0 && row.some(Boolean));
                    if (rows.length > 0) blocks.push({type: 'table', rows});
                } else if (element.matches('.kpi-card')) {
                    blocks.push({type: 'metric', text: element.textContent.trim()});
                } else {
                    const text = element.textContent.trim();
                    if (text) blocks.push({
                        type: element.getAttribute('role') === 'status' ? 'status' : 'paragraph',
                        text
                    });
                }
            });
            return blocks;
        }

        async function exportSelectedTabToPdf() {
            const printButton = document.getElementById('printButton');
            const originalButtonText = printButton.textContent;
            printButton.disabled = true;
            printButton.textContent = 'Preparing selected tab…';
            try {
                await loadTabData(activeTab);
                const tabTitles = {
                    overview: 'Executive KPIs',
                    funnel: 'Funnel Retention',
                    heatmap: 'Transition Matrix',
                    sankey: 'Sankey Flow',
                    search: 'Session Explorer',
                    help: 'Help & How-to'
                };
                const panel = document.getElementById(`panel-${activeTab}`);
                const response = await fetch('/api/export-pdf', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({
                        tab: activeTab,
                        title: `Trishula - ${tabTitles[activeTab] || 'Analysis'}`,
                        dataset: stateData?.parquet_file || 'No active dataset',
                        dedupe: document.getElementById('dedupeSelect').selectedOptions[0]?.textContent || '-',
                        generated_at: new Date().toLocaleString(),
                        blocks: collectPdfBlocks(panel)
                    })
                });
                if (!response.ok) {
                    const error = await response.json();
                    throw new Error(error.detail || 'PDF export failed');
                }
                const blob = await response.blob();
                const url = URL.createObjectURL(blob);
                const link = document.createElement('a');
                link.href = url;
                link.download = `trishula-${activeTab}.pdf`;
                document.body.appendChild(link);
                link.click();
                link.remove();
                URL.revokeObjectURL(url);
            } catch (error) {
                alert(`Unable to export PDF: ${error.message}`);
            } finally {
                printButton.disabled = false;
                printButton.textContent = originalButtonText;
            }
        }

        async function handleBrowserFileUpload(files) {
            if (!files || files.length === 0) return;
            const file = files[0];
            const errDiv = document.getElementById('fileError');
            errDiv.style.display = 'none';
            const loading = startLoading({
                title: 'Loading dataset',
                stage: `Uploading ${file.name} (${(file.size / (1024 * 1024)).toFixed(2)} MB)…`,
                controls: [document.getElementById('browserFileButton')],
                showDelayMs: 150
            });
            try {
                const data = await new Promise((resolve, reject) => {
                    const formData = new FormData();
                    formData.append('file', file);
                    const request = new XMLHttpRequest();
                    request.open('POST', '/api/upload-file');
                    request.responseType = 'json';
                    request.upload.addEventListener('progress', event => {
                        if (event.lengthComputable) {
                            loading.setProgress((event.loaded / event.total) * 100);
                        }
                    });
                    request.upload.addEventListener('load', () => {
                        loading.setStage('Upload complete. Validating schema and preparing Parquet…');
                        loading.setProgress(null);
                    });
                    request.addEventListener('load', () => {
                        const response = request.response || {};
                        if (request.status >= 200 && request.status < 300) resolve(response);
                        else reject(new Error(response.detail || 'Upload failed'));
                    });
                    request.addEventListener('error', () => reject(new Error('Upload connection failed')));
                    request.addEventListener('abort', () => reject(new Error('Upload was cancelled')));
                    request.send(formData);
                });
                loading.setStage('Dataset ready. Loading overview…');
                await fetchState();
            } catch (err) {
                errDiv.innerText = err.message;
                errDiv.style.display = 'block';
            } finally {
                await loading.finish();
            }
        }

        function toggleFileLoader() {
            const loaderCard = document.getElementById('loaderCard');
            const loadButton = document.getElementById('openFileButton');
            const shouldShow = window.getComputedStyle(loaderCard).display === 'none';
            loaderCard.style.display = shouldShow ? 'block' : 'none';
            loadButton.setAttribute('aria-expanded', String(shouldShow));
        }

        function switchTab(tabName) {
            document.querySelectorAll('.nav-btn').forEach(btn => btn.classList.remove('active'));
            document.querySelectorAll('.tab-panel').forEach(panel => panel.classList.remove('active'));

            const tabButton = document.querySelector(`[data-tab="${tabName}"]`);
            if (tabButton) tabButton.classList.add('active');
            document.getElementById(`panel-${tabName}`).classList.add('active');
            activeTab = tabName;
            loadTabData(tabName);
        }

        function onDedupeChange() {
            if (stateData && stateData.loaded) {
                loadedTabs.delete('funnel');
                loadedTabs.delete('heatmap');
                loadedTabs.delete('sankey');
                if (activeTab === 'funnel' || activeTab === 'heatmap' || activeTab === 'sankey') {
                    loadTabData(activeTab, true);
                }
            }
        }

        function resetTabLoads() {
            loadedTabs.clear();
            tabLoadPromises.clear();
        }

        function loadTabData(tabName, force = false) {
            if (!stateData || !stateData.loaded || tabName === 'help') return Promise.resolve();
            if (!force && loadedTabs.has(tabName)) return Promise.resolve();
            if (tabLoadPromises.has(tabName)) {
                const currentRequest = tabLoadPromises.get(tabName);
                return force
                    ? currentRequest.then(() => loadTabData(tabName, true))
                    : currentRequest;
            }

            const loaders = {
                overview: loadInsights,
                funnel: loadEvents,
                heatmap: loadHeatmap,
                sankey: loadSankey,
                search: runSearch
            };
            const loader = loaders[tabName];
            if (!loader) return Promise.resolve();

            const request = Promise.resolve()
                .then(loader)
                .then(() => loadedTabs.add(tabName))
                .catch(err => console.error(`Unable to load ${tabName} tab`, err))
                .finally(() => tabLoadPromises.delete(tabName));
            tabLoadPromises.set(tabName, request);
            return request;
        }

        async function loadInsights() {
            const loading = startLoading({
                title: 'Loading Executive KPIs',
                stage: 'Scanning sessions and calculating summary metrics…'
            });
            try {
                const res = await fetch('/api/insights');
                if (!res.ok) throw new Error('Executive KPI calculation failed');
                const data = await res.json();

                document.getElementById('kpi-sessions').innerText = data.summary.total_sessions.toLocaleString();
                document.getElementById('kpi-bounce').innerText = `${data.summary.bounce_rate_pct}%`;
                document.getElementById('kpi-avg').innerText = data.summary.avg_events_per_session;
                document.getElementById('kpi-median').innerText = `${data.summary.median_events} events`;
                document.getElementById('kpi-p90').innerText = `${data.summary.p90_events} events`;

                const entryTbody = document.querySelector('#entryTable tbody');
                entryTbody.innerHTML = data.entry_points.map(e => `
                    <tr>
                        <td><strong style="color: #f8fafc">${escapeHtml(e.event_name)}</strong></td>
                        <td>${e.entry_count.toLocaleString()}</td>
                        <td><span class="tag-pill">${e.entry_share_pct}%</span></td>
                    </tr>
                `).join('');

                const exitTbody = document.querySelector('#exitTable tbody');
                exitTbody.innerHTML = data.exit_points.map(e => `
                    <tr>
                        <td><strong style="color: #f8fafc">${escapeHtml(e.event_name)}</strong></td>
                        <td>${e.exit_count.toLocaleString()}</td>
                        <td><span class="tag-pill" style="color: #fb7185; border-color: rgba(251,113,133,0.3)">${e.exit_share_pct}%</span></td>
                    </tr>
                `).join('');
            } finally {
                await loading.finish();
            }
        }

        async function loadEvents() {
            const dedupe = document.getElementById('dedupeSelect').value;
            const select = document.getElementById('addEventSelect');
            const status = document.getElementById('funnelEventStatus');
            const presetButtons = document.querySelectorAll('.funnel-preset');
            const loading = startLoading({
                title: 'Loading Funnel Retention',
                stage: 'Discovering the most frequent event steps…'
            });
            status.style.color = '#94a3b8';
            status.textContent = 'Loading frequent events…';
            presetButtons.forEach(button => button.disabled = true);
            select.disabled = true;
            select.replaceChildren();
            const placeholderOption = document.createElement('option');
            placeholderOption.value = '';
            placeholderOption.textContent = 'Loading events…';
            select.appendChild(placeholderOption);
            try {
                const res = await fetch(`/api/events?dedupe=${dedupe}`);
                const contentType = res.headers.get('content-type') || '';
                const data = contentType.includes('application/json')
                    ? await res.json()
                    : {detail: await res.text()};
                if (!res.ok) throw new Error(data.detail || 'Event discovery failed');
                if (!Array.isArray(data)) throw new Error('Invalid event response');

                allTopEventsList = data;
                placeholderOption.textContent = data.length
                    ? '+ Select Event Step to Add...'
                    : 'No events available';
                data.forEach(e => {
                    const option = document.createElement('option');
                    option.value = e.event_name;
                    option.textContent = `${e.event_name} (${e.occurrence_count.toLocaleString()} occurrences)`;
                    select.appendChild(option);
                });

                const chipsDiv = document.getElementById('quickSearchChips');
                chipsDiv.replaceChildren();
                data.slice(0, 6).forEach(e => {
                    const button = document.createElement('button');
                    button.className = 'chip-btn';
                    button.textContent = `Event: ${e.event_name}`;
                    button.addEventListener('click', () => applySearchFilter(e.event_name, ''));
                    chipsDiv.appendChild(button);
                });

                if (!data.length) {
                    status.textContent = 'No usable events were found in the active dataset.';
                    selectedFunnelSteps = [];
                    renderFunnelPills();
                    return;
                }

                status.style.color = '#34d399';
                status.textContent = `Loaded ${data.length} frequent events. Choose a preset or add steps, then calculate.`;
                presetButtons.forEach(button => button.disabled = false);
                select.disabled = false;
                renderFunnelPills();
            } catch (err) {
                allTopEventsList = [];
                selectedFunnelSteps = [];
                status.style.color = '#fb7185';
                status.textContent = `Unable to load events: ${err.message}`;
                placeholderOption.textContent = 'Events unavailable';
                renderFunnelPills();
                throw err;
            } finally {
                await loading.finish();
            }
        }

        function renderFunnelPills() {
            const container = document.getElementById('funnelPills');
            document.getElementById('calculateFunnelButton').disabled = selectedFunnelSteps.length === 0;
            if (selectedFunnelSteps.length === 0) {
                container.innerHTML = `<span style="color: #64748b; font-size: 13px;">No funnel steps selected. Click a preset above or add a step!</span>`;
                return;
            }
            container.innerHTML = selectedFunnelSteps.map((step, idx) => `
                <span class="tag-pill" style="padding: 8px 16px; font-size: 14px; background: rgba(56, 189, 248, 0.15);">
                    <strong>#${idx+1}</strong> ${escapeHtml(step)}
                    <button type="button" class="remove-funnel-step" data-index="${idx}" style="cursor: pointer; margin-left: 8px; font-weight: bold; color: #fb7185; background: none; border: 0;">✕</button>
                </span>
            `).join('');
            container.querySelectorAll('.remove-funnel-step').forEach(button => {
                button.addEventListener('click', () => removeStepFromFunnel(Number(button.dataset.index)));
            });
        }

        function applyFunnelPreset(presetType) {
            if (allTopEventsList.length === 0) {
                const status = document.getElementById('funnelEventStatus');
                status.style.color = '#fb7185';
                status.textContent = 'Events are not available yet. Wait for loading to finish or review the error above.';
                return;
            }
            if (presetType === 'top') {
                selectedFunnelSteps = allTopEventsList.slice(0, 4).map(e => e.event_name);
            } else if (presetType === 'checkout') {
                const checkoutCandidates = ['Home', 'Search', 'Product_View', 'Add_To_Cart', 'Checkout', 'Payment', 'Order_Confirmation'];
                selectedFunnelSteps = checkoutCandidates.filter(c => allTopEventsList.some(e => e.event_name === c));
                if (selectedFunnelSteps.length === 0) selectedFunnelSteps = allTopEventsList.slice(0, 4).map(e => e.event_name);
            } else if (presetType === 'search') {
                const searchCandidates = ['Search', 'Product_View', 'Category_Browse', 'Add_To_Cart'];
                selectedFunnelSteps = searchCandidates.filter(c => allTopEventsList.some(e => e.event_name === c));
                if (selectedFunnelSteps.length === 0) selectedFunnelSteps = allTopEventsList.slice(0, 3).map(e => e.event_name);
            }
            renderFunnelPills();
            loadFunnel();
        }

        function clearFunnel() {
            selectedFunnelSteps = [];
            renderFunnelPills();
            const tbody = document.querySelector('#funnelMetricsTable tbody');
            tbody.innerHTML = `<tr><td colspan="6" style="text-align: center; color: #64748b;">Add at least one step to compute funnel metrics.</td></tr>`;
        }

        function addStepToFunnel(stepName) {
            if (stepName) {
                selectedFunnelSteps.push(stepName);
                renderFunnelPills();
                const status = document.getElementById('funnelEventStatus');
                status.style.color = '#94a3b8';
                status.textContent = 'Funnel steps changed. Select Calculate Funnel when ready.';
            }
            document.getElementById('addEventSelect').value = "";
        }

        function removeStepFromFunnel(idx) {
            selectedFunnelSteps.splice(idx, 1);
            renderFunnelPills();
            if (selectedFunnelSteps.length === 0) {
                clearFunnel();
                return;
            }
            const status = document.getElementById('funnelEventStatus');
            status.style.color = '#94a3b8';
            status.textContent = 'Funnel steps changed. Select Calculate Funnel when ready.';
        }

        async function loadFunnel() {
            if (selectedFunnelSteps.length === 0) return;
            const dedupe = document.getElementById('dedupeSelect').value;
            const stepsParam = selectedFunnelSteps.join(',');
            const status = document.getElementById('funnelEventStatus');
            const tbody = document.querySelector('#funnelMetricsTable tbody');
            const loading = startLoading({
                title: 'Calculating Funnel Retention',
                stage: `Matching ${selectedFunnelSteps.length} ordered steps across sessions…`,
                controls: [document.getElementById('calculateFunnelButton')]
            });
            try {
                status.style.color = '#94a3b8';
                status.textContent = 'Calculating funnel retention…';
                const res = await fetch(`/api/funnel?steps=${encodeURIComponent(stepsParam)}&dedupe=${dedupe}`);
                const contentType = res.headers.get('content-type') || '';
                const data = contentType.includes('application/json')
                    ? await res.json()
                    : {detail: await res.text()};
                if (!res.ok) throw new Error(data.detail || 'Funnel calculation failed');
                if (!Array.isArray(data)) throw new Error('Invalid funnel response');

                tbody.innerHTML = data.map(r => `
                    <tr>
                        <td><strong>#${r.step_number}</strong></td>
                        <td><strong style="color: #38bdf8">${escapeHtml(r.step_name)}</strong></td>
                        <td><strong>${r.session_count.toLocaleString()}</strong></td>
                        <td><span class="tag-pill" style="color: #34d399">${r.step_conversion_pct}%</span></td>
                        <td><span class="tag-pill" style="color: #fb7185; border-color: rgba(251,113,133,0.3)">${r.step_dropoff_pct}%</span></td>
                        <td>
                            <div class="bar-container">
                                <div class="bar-fill" style="width: ${r.step_conversion_pct}%"></div>
                            </div>
                        </td>
                    </tr>
                `).join('');
                status.style.color = '#34d399';
                status.textContent = `Funnel calculated for ${data.length} steps.`;
            } catch (err) {
                status.style.color = '#fb7185';
                status.textContent = `Unable to calculate funnel: ${err.message}`;
                tbody.innerHTML = `<tr><td colspan="6" style="text-align: center; color: #fb7185;">Funnel calculation failed.</td></tr>`;
            } finally {
                await loading.finish();
            }
        }

        async function loadHeatmap() {
            const dedupe = document.getElementById('dedupeSelect').value;
            const table = document.getElementById('heatmapTable');
            const status = document.getElementById('heatmapStatus');
            const thead = document.querySelector('#heatmapTable thead');
            const tbody = document.querySelector('#heatmapTable tbody');
            const loading = startLoading({
                title: 'Building Transition Matrix',
                stage: 'Counting event-to-event transitions across sessions…'
            });
            status.style.color = '#94a3b8';
            status.textContent = 'Calculating transition matrix…';
            status.style.display = 'block';
            table.style.display = 'none';
            thead.innerHTML = '';
            tbody.innerHTML = '';

            try {
                const res = await fetch(`/api/heatmap?dedupe=${dedupe}`);
                const contentType = res.headers.get('content-type') || '';
                const data = contentType.includes('application/json')
                    ? await res.json()
                    : {detail: await res.text()};
                if (!res.ok) throw new Error(data.detail || 'Transition calculation failed');
                if (!data.columns?.length || !data.index?.length || !data.data?.length) {
                    status.textContent = 'No events are available for a transition matrix.';
                    return;
                }

                const values = data.data.flat();
                const maxVal = Math.max(0, ...values);
                thead.innerHTML = `<tr><th>From / To</th>${data.columns.map(c => `<th>${escapeHtml(c)}</th>`).join('')}</tr>`;
                tbody.innerHTML = data.index.map((rowLabel, rIdx) => `
                    <tr>
                        <td><strong style="color: #38bdf8">${escapeHtml(rowLabel)}</strong></td>
                        ${data.data[rIdx].map(val => {
                            const intensity = maxVal > 0 ? val / maxVal : 0;
                            const bg = val > 0 ? `rgba(56, 189, 248, ${Math.max(0.15, intensity)})` : 'rgba(30, 41, 59, 0.4)';
                            return `<td style="background: ${bg}; text-align: center; font-weight: bold;">${val > 0 ? val.toLocaleString() : '-'}</td>`;
                        }).join('')}
                    </tr>
                `).join('');
                table.style.display = 'table';
                status.textContent = maxVal > 0
                    ? `Showing transitions among the ${data.columns.length} most frequent events.`
                    : 'Events were found, but there are no transitions between them.';
            } catch (err) {
                status.style.color = '#fb7185';
                status.textContent = `Unable to load transition matrix: ${err.message}`;
            } finally {
                await loading.finish();
            }
        }

        function createSvgElement(name, attributes = {}) {
            const element = document.createElementNS('http://www.w3.org/2000/svg', name);
            Object.entries(attributes).forEach(([key, value]) => element.setAttribute(key, String(value)));
            return element;
        }

        function renderSankey(matrix) {
            const chart = document.getElementById('sankeyChart');
            chart.replaceChildren();

            const links = [];
            matrix.index.forEach((source, sourceIndex) => {
                matrix.columns.forEach((target, targetIndex) => {
                    const value = Number(matrix.data[sourceIndex]?.[targetIndex] || 0);
                    if (value > 0) links.push({source, target, value});
                });
            });
            links.sort((left, right) => right.value - left.value);
            const strongestLinks = links.slice(0, 24);
            if (!strongestLinks.length) return 0;

            const sourceNames = [...new Set(strongestLinks.map(link => link.source))];
            const targetNames = [...new Set(strongestLinks.map(link => link.target))];
            const sourceTotals = new Map(sourceNames.map(name => [name, 0]));
            const targetTotals = new Map(targetNames.map(name => [name, 0]));
            strongestLinks.forEach(link => {
                sourceTotals.set(link.source, sourceTotals.get(link.source) + link.value);
                targetTotals.set(link.target, targetTotals.get(link.target) + link.value);
            });

            const width = 1100;
            const height = 600;
            const top = 24;
            const bottom = 24;
            const gap = 12;
            const sourceX = 210;
            const targetX = 890;
            const nodeWidth = 18;
            const availableHeight = height - top - bottom;
            const totalFlow = strongestLinks.reduce((sum, link) => sum + link.value, 0);
            const sourceScale = (availableHeight - gap * Math.max(0, sourceNames.length - 1)) / totalFlow;
            const targetScale = (availableHeight - gap * Math.max(0, targetNames.length - 1)) / totalFlow;
            const scale = Math.max(0, Math.min(sourceScale, targetScale));
            const palette = ['#38bdf8', '#818cf8', '#34d399', '#fbbf24', '#fb7185', '#22d3ee', '#c084fc', '#4ade80', '#f97316', '#60a5fa'];

            function positionNodes(names, totals) {
                const usedHeight = names.reduce((sum, name) => sum + totals.get(name) * scale, 0)
                    + gap * Math.max(0, names.length - 1);
                let cursor = top + Math.max(0, (availableHeight - usedHeight) / 2);
                return new Map(names.map(name => {
                    const node = {name, y: cursor, height: totals.get(name) * scale, offset: 0};
                    cursor += node.height + gap;
                    return [name, node];
                }));
            }

            const sources = positionNodes(sourceNames, sourceTotals);
            const targets = positionNodes(targetNames, targetTotals);
            const svg = createSvgElement('svg', {
                viewBox: `0 0 ${width} ${height}`,
                role: 'img',
                'aria-labelledby': 'sankeySvgTitle sankeySvgDescription'
            });
            const title = createSvgElement('title', {id: 'sankeySvgTitle'});
            title.textContent = 'Event transition Sankey diagram';
            const description = createSvgElement('desc', {id: 'sankeySvgDescription'});
            description.textContent = `${strongestLinks.length} strongest event transitions. Link width represents transition count.`;
            svg.append(title, description);

            strongestLinks.forEach((link, index) => {
                const source = sources.get(link.source);
                const target = targets.get(link.target);
                const linkWidth = link.value * scale;
                const sourceY = source.y + source.offset + linkWidth / 2;
                const targetY = target.y + target.offset + linkWidth / 2;
                source.offset += linkWidth;
                target.offset += linkWidth;
                const path = createSvgElement('path', {
                    d: `M ${sourceX + nodeWidth} ${sourceY} C 520 ${sourceY}, 580 ${targetY}, ${targetX} ${targetY}`,
                    stroke: palette[sourceNames.indexOf(link.source) % palette.length],
                    'stroke-width': Math.max(1, linkWidth),
                    class: 'sankey-link'
                });
                const pathTitle = createSvgElement('title');
                pathTitle.textContent = `${link.source} → ${link.target}: ${link.value.toLocaleString()} transitions`;
                path.appendChild(pathTitle);
                svg.appendChild(path);
            });

            function appendNodes(nodes, x, names, labelSide) {
                names.forEach((name, index) => {
                    const node = nodes.get(name);
                    const color = labelSide === 'source'
                        ? palette[index % palette.length]
                        : '#a5b4fc';
                    const rect = createSvgElement('rect', {
                        x,
                        y: node.y,
                        width: nodeWidth,
                        height: Math.max(1, node.height),
                        rx: 3,
                        fill: color,
                        class: 'sankey-node'
                    });
                    const label = createSvgElement('text', {
                        x: labelSide === 'source' ? x - 10 : x + nodeWidth + 10,
                        y: node.y + Math.max(10, node.height / 2 + 4),
                        'text-anchor': labelSide === 'source' ? 'end' : 'start',
                        class: 'sankey-label'
                    });
                    label.textContent = name;
                    svg.append(rect, label);
                });
            }

            appendNodes(sources, sourceX, sourceNames, 'source');
            appendNodes(targets, targetX, targetNames, 'target');
            chart.appendChild(svg);
            return strongestLinks.length;
        }

        async function loadSankey() {
            const dedupe = document.getElementById('dedupeSelect').value;
            const status = document.getElementById('sankeyStatus');
            const chart = document.getElementById('sankeyChart');
            const loading = startLoading({
                title: 'Building Sankey Flow',
                stage: 'Ranking the strongest event-to-event transitions…'
            });
            status.style.color = '#94a3b8';
            status.textContent = 'Calculating Sankey flow…';
            chart.replaceChildren();
            try {
                const res = await fetch(`/api/heatmap?top=10&dedupe=${dedupe}`);
                const contentType = res.headers.get('content-type') || '';
                const data = contentType.includes('application/json')
                    ? await res.json()
                    : {detail: await res.text()};
                if (!res.ok) throw new Error(data.detail || 'Sankey calculation failed');
                if (!data.columns?.length || !data.index?.length || !data.data?.length) {
                    status.textContent = 'No events are available for a Sankey diagram.';
                    return;
                }
                const linkCount = renderSankey(data);
                status.textContent = linkCount
                    ? `Showing the ${linkCount} strongest transitions. Hover a link for its exact count.`
                    : 'Events were found, but there are no transitions between them.';
            } catch (err) {
                status.style.color = '#fb7185';
                status.textContent = `Unable to load Sankey flow: ${err.message}`;
            } finally {
                await loading.finish();
            }
        }

        function applySearchFilter(eventVal, subpathVal) {
            document.getElementById('searchEventInput').value = eventVal;
            document.getElementById('searchSubpathInput').value = subpathVal;
            runSearch();
        }

        function splitJourney(eventPath) {
            const delimiter = stateData?.delimiter || '->';
            return String(eventPath || '').split(delimiter).map(step => step.trim()).filter(Boolean);
        }

        function matchingJourneyIndexes(steps) {
            const matches = new Set();
            const eventFilter = document.getElementById('searchEventInput').value.trim();
            if (eventFilter) {
                steps.forEach((step, index) => {
                    if (step === eventFilter) matches.add(index);
                });
            }

            const delimiter = stateData?.delimiter || '->';
            const subpath = document.getElementById('searchSubpathInput').value.trim();
            const subpathSteps = subpath
                ? subpath.split(delimiter).map(step => step.trim()).filter(Boolean)
                : [];
            if (subpathSteps.length) {
                for (let start = 0; start <= steps.length - subpathSteps.length; start += 1) {
                    if (subpathSteps.every((step, offset) => steps[start + offset] === step)) {
                        subpathSteps.forEach((_, offset) => matches.add(start + offset));
                    }
                }
            }
            return matches;
        }

        function compressJourney(steps, matches) {
            const groups = [];
            steps.forEach((step, index) => {
                const previous = groups[groups.length - 1];
                if (previous && previous.name === step) {
                    previous.count += 1;
                    previous.matched = previous.matched || matches.has(index);
                    return;
                }
                groups.push({name: step, count: 1, matched: matches.has(index)});
            });
            return groups;
        }

        function journeyGroupHtml(group) {
            if (group.overflow) {
                return `<span class="journey-overflow">… ${group.count} grouped events …</span>`;
            }
            const matchClass = group.matched ? ' match' : '';
            const count = group.count > 1
                ? `<span class="breadcrumb-count">×${group.count}</span>`
                : '';
            return `<span class="breadcrumb-pill${matchClass}">${escapeHtml(group.name)}${count}</span>`;
        }

        function compactJourneyGroups(groups) {
            const previewLimit = 8;
            if (groups.length <= previewLimit) return groups;
            const preview = groups.slice(0, previewLimit);
            const firstHiddenMatch = groups.findIndex((group, index) => index >= previewLimit && group.matched);
            if (firstHiddenMatch >= 0) {
                if (firstHiddenMatch > previewLimit) {
                    preview.push({overflow: true, count: firstHiddenMatch - previewLimit});
                }
                preview.push(groups[firstHiddenMatch]);
            }
            return preview;
        }

        function renderSessionResults() {
            const tbody = document.querySelector('#searchTable tbody');
            const controls = document.getElementById('sessionControls');
            controls.classList.toggle('visible', currentSessionResults.length > 0);
            document.getElementById('collapseAllSessionsButton').disabled = expandedSessionRows.size === 0;
            tbody.innerHTML = currentSessionResults.map((session, index) => {
                const steps = splitJourney(session.EVENT_PATH);
                const groups = compressJourney(steps, matchingJourneyIndexes(steps));
                const expanded = expandedSessionRows.has(index);
                const visibleGroups = expanded ? groups : compactJourneyGroups(groups);
                const breadcrumbs = visibleGroups.map(journeyGroupHtml)
                    .join('<span class="breadcrumb-arrow">➔</span>');
                const hiddenCount = Math.max(groups.length - visibleGroups.filter(group => !group.overflow).length, 0);
                const toggleText = expanded
                    ? 'Collapse journey'
                    : `Show complete journey (${groups.length} grouped steps)`;

                return `
                    <tr>
                        <td><strong class="session-id">${escapeHtml(session.SESSION)}</strong></td>
                        <td class="journey-cell${expanded ? ' expanded' : ''}">
                            <div class="journey-flow">${breadcrumbs}</div>
                            ${groups.length > 8 ? `
                                <button type="button" class="journey-toggle" data-session-index="${index}" aria-expanded="${expanded}">
                                    ${escapeHtml(toggleText)}${!expanded && hiddenCount ? ` · ${hiddenCount} hidden` : ''}
                                </button>
                            ` : ''}
                        </td>
                        <td><span class="tag-pill">${session.TOTAL_EVENTS} events</span></td>
                    </tr>
                `;
            }).join('');
            tbody.querySelectorAll('.journey-toggle').forEach(button => {
                button.addEventListener('click', () => {
                    const index = Number(button.dataset.sessionIndex);
                    if (expandedSessionRows.has(index)) expandedSessionRows.delete(index);
                    else expandedSessionRows.add(index);
                    renderSessionResults();
                });
            });
        }

        async function runSearch() {
            const ev = document.getElementById('searchEventInput').value.trim();
            const sub = document.getElementById('searchSubpathInput').value.trim();
            const status = document.getElementById('searchStatus');
            const tbody = document.querySelector('#searchTable tbody');
            const loading = startLoading({
                title: 'Searching Sessions',
                stage: 'Filtering navigation journeys and preparing compact previews…',
                controls: [document.getElementById('searchButton')]
            });
            let url = '/api/search?limit=25';
            if (ev) url += `&event=${encodeURIComponent(ev)}`;
            if (sub) url += `&subpath=${encodeURIComponent(sub)}`;
            try {
                status.style.color = '#94a3b8';
                status.textContent = 'Searching sessions…';
                const res = await fetch(url);
                const contentType = res.headers.get('content-type') || '';
                const data = contentType.includes('application/json')
                    ? await res.json()
                    : {detail: await res.text()};
                if (!res.ok) throw new Error(data.detail || 'Session search failed');
                if (!Array.isArray(data)) throw new Error('Invalid search response');

                if (data.length === 0) {
                    currentSessionResults = [];
                    expandedSessionRows.clear();
                    renderSessionResults();
                    status.textContent = 'No matching sessions found.';
                    tbody.innerHTML = `<tr><td colspan="3" style="text-align: center; color: #64748b;">No matching sessions found.</td></tr>`;
                    return;
                }

                currentSessionResults = data;
                expandedSessionRows.clear();
                renderSessionResults();
                status.style.color = '#34d399';
                status.textContent = `Showing ${data.length} matching sessions.`;
            } catch (err) {
                currentSessionResults = [];
                expandedSessionRows.clear();
                renderSessionResults();
                status.style.color = '#fb7185';
                status.textContent = `Unable to search sessions: ${err.message}`;
                tbody.innerHTML = `<tr><td colspan="3" style="text-align: center; color: #fb7185;">Session search failed.</td></tr>`;
            } finally {
                await loading.finish();
            }
        }
