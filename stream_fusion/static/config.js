const sorts = ['quality', 'sizedesc', 'sizeasc', 'qualitythensize'];
const qualityExclusions = ['2160p', '1080p', '720p', '480p', 'rips', 'cam', 'hevc', 'unknown'];
const languages = ['en', 'fr', 'multi', 'vfq'];


const implementedDebrids = ['debrid_rd', 'debrid_ad', 'debrid_tb', 'debrid_pm', 'utopeer'];

const unimplementedDebrids = ['debrid_dl', 'debrid_ed', 'debrid_oc', 'debrid_pk'];

document.addEventListener('DOMContentLoaded', function () {
    loadData();
    handleUniqueAccounts();
    updateProviderFields();
    updateDebridOrderList();
    toggleStremThruFields();

    // Appliqué en dernier via setTimeout(0) pour garantir que tous les appels
    // synchrones précédents (updateProviderFields, etc.) sont terminés avant
    // de figer l'état visuel des boutons OAuth sur les tokens déjà présents.
    setTimeout(restoreAuthStates, 0);

    const apiKeyInput = document.getElementById('ApiKey');
    if (apiKeyInput) {
        apiKeyInput.addEventListener('blur', function() {
            validateApiKeyWithoutAlert(this.value);
        });

        if (apiKeyInput.value && apiKeyInput.value.trim() !== '') {
            validateApiKeyWithoutAlert(apiKeyInput.value);
        }
    }
});

function setElementDisplay(elementId, displayStatus) {
    const element = document.getElementById(elementId);
    if (element) {
        element.style.display = displayStatus;
    }
}

function startRealDebridAuth() {
    document.getElementById('rd-auth-button').disabled = true;
    document.getElementById('rd-auth-button').textContent = "Authentification en cours...";

    fetch('/api/auth/realdebrid/device_code', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({})
    })
        .then(response => {
            if (!response.ok) {
                throw new Error('Erreur de requête');
            }
            return response.json();
        })
        .then(data => {
            document.getElementById('verification-url').href = data.direct_verification_url;
            document.getElementById('verification-url').textContent = data.verification_url;
            document.getElementById('user-code').textContent = data.user_code;
            document.getElementById('auth-instructions').style.display = 'block';
            pollForCredentials(data.device_code, data.expires_in);
        })
        .catch(error => {
            alert("Erreur lors de l'authentification. Veuillez réessayer.");
            resetAuthButton();
        });
}

function pollForCredentials(deviceCode, expiresIn) {
    const pollInterval = setInterval(() => {
        fetch(`/api/auth/realdebrid/credentials?device_code=${encodeURIComponent(deviceCode)}`, {
            method: 'POST',
            headers: {
                'accept': 'application/json'
            }
        })
            .then(response => {
                if (!response.ok) {
                    if (response.status === 400) {
                        console.log('Autorisation en attente...');
                        return null;
                    }
                    throw new Error('Erreur de requête');
                }
                return response.json();
            })
            .then(data => {
                if (data && data.client_id && data.client_secret) {
                    clearInterval(pollInterval);
                    clearTimeout(timeoutId);
                    getToken(deviceCode, data.client_id, data.client_secret);
                }
            })
            .catch(error => {
                console.error('Erreur:', error);
                console.log('Tentative suivante dans 5 secondes...');
            });
    }, 5000);

    const timeoutId = setTimeout(() => {
        clearInterval(pollInterval);
        alert("Le délai d'authentification a expiré. Veuillez réessayer.");
        resetAuthButton();
    }, expiresIn * 1000);
}

function getToken(deviceCode, clientId, clientSecret) {
    const url = `/api/auth/realdebrid/token?client_id=${encodeURIComponent(clientId)}&client_secret=${encodeURIComponent(clientSecret)}&device_code=${encodeURIComponent(deviceCode)}`;

    fetch(url, {
        method: 'POST',
        headers: {
            'accept': 'application/json'
        }
    })
        .then(response => {
            if (!response.ok) {
                throw new Error('Erreur de requête');
            }
            return response.json();
        })
        .then(data => {
            if (data.access_token && data.refresh_token) {
                const rdCredentials = {
                    client_id: clientId,
                    client_secret: clientSecret,
                    access_token: data.access_token,
                    refresh_token: data.refresh_token
                };
                document.getElementById('rd_token_info').value = JSON.stringify(rdCredentials, null, 2);
                document.getElementById('auth-status').style.display = 'block';
                document.getElementById('auth-instructions').style.display = 'none';
                document.getElementById('rd-auth-button').disabled = true;
                document.getElementById('rd-auth-button').classList.add('opacity-50', 'cursor-not-allowed');
                document.getElementById('rd-auth-button').textContent = "Connexion réussie";
            } else {
                throw new Error('Tokens non reçus');
            }
        })
        .catch(error => {
            console.error('Erreur:', error);
            console.log('Erreur lors de la récupération du token. Nouvelle tentative lors du prochain polling.');
        });
}

function resetAuthButton() {
    const button = document.getElementById('rd-auth-button');
    button.disabled = false;
    button.textContent = "S'authentifier avec Real-Debrid";
    button.classList.remove('opacity-50', 'cursor-not-allowed');
}

function startADAuth() {
    document.getElementById('ad-auth-button').disabled = true;
    document.getElementById('ad-auth-button').textContent = "Authentication in progress...";

    console.log('Starting AllDebrid authentication');
    fetch('/api/auth/alldebrid/pin/get', {
        method: 'GET',
        headers: {
            'Content-Type': 'application/json'
        }
    })
        .then(response => {
            console.log('Response received', response);
            if (!response.ok) {
                throw new Error('Request error');
            }
            return response.json();
        })
        .then(data => {
            document.getElementById('ad-verification-url').href = data.data.user_url;
            document.getElementById('ad-verification-url').textContent = data.data.base_url;
            document.getElementById('ad-user-code').textContent = data.data.pin;
            document.getElementById('ad-auth-instructions').style.display = 'block';
            pollForADCredentials(data.data.check, data.data.pin, data.data.expires_in);
        })
        .catch(error => {
            console.error('Detailed error:', error);
            alert("Authentication error. Please try again.");
            resetADAuthButton();
        });
}

function pollForADCredentials(check, pin, expiresIn) {
    const pollInterval = setInterval(() => {
        fetch(`/api/auth/alldebrid/pin/check?agent=streamfusion&check=${encodeURIComponent(check)}&pin=${encodeURIComponent(pin)}`, {
            method: 'GET',
            headers: {
                'accept': 'application/json'
            }
        })
            .then(response => {
                if (response.status === 400) {
                    console.log('Waiting for user authorization...');
                    return null;
                }
                if (!response.ok) {
                    throw new Error('Request error');
                }
                return response.json();
            })
            .then(data => {
                if (data === null) return;
                if (data.data && data.data.activated && data.data.apikey) {
                    clearInterval(pollInterval);
                    clearTimeout(timeoutId);
                    document.getElementById('ad_token_info').value = data.data.apikey;
                    document.getElementById('ad-auth-status').style.display = 'block';
                    document.getElementById('ad-auth-instructions').style.display = 'none';
                    document.getElementById('ad-auth-button').disabled = true;
                    document.getElementById('ad-auth-button').textContent = "Connexion établie.";
                    console.log('AllDebrid authentication successful');
                } else {
                    console.log('Waiting for user authorization...');
                }
            })
            .catch(error => {
                console.error('Error:', error);
                console.log('Next attempt in 5 seconds...');
            });
    }, 5000);

    const timeoutId = setTimeout(() => {
        clearInterval(pollInterval);
        alert("Authentication timeout. Please try again.");
        resetADAuthButton();
    }, expiresIn * 1000);
}

function resetADAuthButton() {
    const button = document.getElementById('ad-auth-button');
    button.disabled = false;
    button.textContent = "Connect with AllDebrid";
}

function handleUniqueAccounts() {
    const accounts = ['debrid_rd', 'debrid_ad', 'debrid_tb', 'debrid_pm', 'utopeer', 'c411', 'torr9', 'lacale', 'generationfree', 'abn', 'g3mini', 'theoldschool'];

    accounts.forEach(account => {
        const checkbox = document.getElementById(account);
        if (checkbox) {
            const isUnique = checkbox.dataset.uniqueAccount === 'true';
            if (!isUnique) {
            } else {
                checkbox.checked = isUnique;
                checkbox.disabled = isUnique;
                checkbox.parentElement.classList.add('opacity-50', 'cursor-not-allowed');
            }
        }
    });
}

function updateDebridOrderList() {
    const debridOrderList = document.getElementById('debridOrderList');
    if (!debridOrderList) return;

    debridOrderList.innerHTML = '';

    let debridOrder = [];

    // 1. Priorité : config en localStorage
    const _stored = localStorage.getItem('streamfusion_config');
    if (_stored) {
        try { debridOrder = JSON.parse(_stored).service || []; } catch (e) { /* ignore */ }
    }

    // 2. Fallback : anciennes URLs base64
    if (debridOrder.length === 0) {
        const _data = window.location.href.match(/\/([^\/]+)\/configure$/);
        if (_data && _data[1]) {
            try {
                debridOrder = JSON.parse(atob(_data[1])).service || [];
            } catch (error) {
                console.warn("No valid debrid order data in URL, using default order.");
            }
        }
    }

    const rdEnabled = document.getElementById('debrid_rd').checked || document.getElementById('debrid_rd').disabled;
    const adEnabled = document.getElementById('debrid_ad').checked || document.getElementById('debrid_ad').disabled;
    const tbEnabled = document.getElementById('debrid_tb').checked || document.getElementById('debrid_tb').disabled;
    const pmEnabled = document.getElementById('debrid_pm').checked || document.getElementById('debrid_pm').disabled;
    const dlEnabled = document.getElementById('debrid_dl')?.checked || document.getElementById('debrid_dl')?.disabled;
    const edEnabled = document.getElementById('debrid_ed')?.checked || document.getElementById('debrid_ed')?.disabled;
    const ocEnabled = document.getElementById('debrid_oc')?.checked || document.getElementById('debrid_oc')?.disabled;
    const pkEnabled = document.getElementById('debrid_pk')?.checked || document.getElementById('debrid_pk')?.disabled;

    if (debridOrder.length === 0 ||
        !debridOrder.every(service =>
            (service === 'Real-Debrid' && rdEnabled) ||
            (service === 'AllDebrid' && adEnabled) ||
            (service === 'TorBox' && tbEnabled) ||
            (service === 'Premiumize' && pmEnabled) ||
            (service === 'Debrid-Link' && dlEnabled) ||
            (service === 'EasyDebrid' && edEnabled) ||
            (service === 'Offcloud' && ocEnabled) ||
            (service === 'PikPak' && pkEnabled)
        )) {
        debridOrder = [];
        if (rdEnabled) debridOrder.push('Real-Debrid');
        if (adEnabled) debridOrder.push('AllDebrid');
        if (tbEnabled) debridOrder.push('TorBox');
        if (pmEnabled) debridOrder.push('Premiumize');
        if (dlEnabled) debridOrder.push('Debrid-Link');
        if (edEnabled) debridOrder.push('EasyDebrid');
        if (ocEnabled) debridOrder.push('Offcloud');
        if (pkEnabled) debridOrder.push('PikPak');
    }

    debridOrder.forEach(serviceName => {
        if ((serviceName === 'Real-Debrid' && rdEnabled) ||
            (serviceName === 'AllDebrid' && adEnabled) ||
            (serviceName === 'Premiumize' && pmEnabled) ||
            (serviceName === 'TorBox' && tbEnabled) ||
            (serviceName === 'Debrid-Link' && dlEnabled) ||
            (serviceName === 'EasyDebrid' && edEnabled) ||
            (serviceName === 'Offcloud' && ocEnabled) ||
            (serviceName === 'PikPak' && pkEnabled)) {
            addDebridToList(serviceName);
        }
    });

    if (rdEnabled && !debridOrder.includes('Real-Debrid')) {
        addDebridToList('Real-Debrid');
    }
    if (adEnabled && !debridOrder.includes('AllDebrid')) {
        addDebridToList('AllDebrid');
    }
    if (tbEnabled && !debridOrder.includes('TorBox')) {
        addDebridToList('TorBox');
    }
    if (pmEnabled && !debridOrder.includes('Premiumize')) {
        addDebridToList('Premiumize');
    }
    if (dlEnabled && !debridOrder.includes('Debrid-Link')) {
        addDebridToList('Debrid-Link');
    }
    if (edEnabled && !debridOrder.includes('EasyDebrid')) {
        addDebridToList('EasyDebrid');
    }
    if (ocEnabled && !debridOrder.includes('Offcloud')) {
        addDebridToList('Offcloud');
    }
    if (pkEnabled && !debridOrder.includes('PikPak')) {
        addDebridToList('PikPak');
    }

    if (typeof window._updateDolPositions === 'function') window._updateDolPositions();
    if (typeof window._updateDolArrows === 'function') window._updateDolArrows();
}


function addDebridToList(serviceName) {
    const debridOrderList = document.getElementById('debridOrderList');
    const li = document.createElement('li');

    const handle = document.createElement('div');
    handle.className = 'dol-handle';
    handle.innerHTML = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="9" cy="6" r="1"/><circle cx="15" cy="6" r="1"/><circle cx="9" cy="12" r="1"/><circle cx="15" cy="12" r="1"/><circle cx="9" cy="18" r="1"/><circle cx="15" cy="18" r="1"/></svg>';

    const content = document.createElement('div');
    content.className = 'dol-content';

    const pos = document.createElement('span');
    pos.className = 'dol-pos';

    const name = document.createElement('span');
    name.className = 'dol-name';
    name.textContent = serviceName;

    content.appendChild(pos);
    content.appendChild(name);

    const arrows = document.createElement('div');
    arrows.className = 'dol-arrows';
    arrows.innerHTML = '<button type="button" class="dol-arrow dol-up" aria-label="Monter"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="18 15 12 9 6 15"/></svg></button>'
        + '<button type="button" class="dol-arrow dol-down" aria-label="Descendre"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="6 9 12 15 18 9"/></svg></button>';

    li.appendChild(handle);
    li.appendChild(content);
    li.appendChild(arrows);
    li.dataset.serviceName = serviceName;
    debridOrderList.appendChild(li);
}

function toggleDebridOrderList() {
    const orderList = document.getElementById('debridOrderList');
    const isChecked = document.getElementById('debrid_order').checked;
    orderList.classList.toggle('hidden', !isChecked);

    if (isChecked) {
        updateDebridOrderList();
    }
}

function toggleStremThruFields() {
    const stremthruEnabledCheckbox = document.getElementById('stremthru_enabled');
    if (!stremthruEnabledCheckbox) return;
    
    const isEnabled = stremthruEnabledCheckbox.checked;
    const urlDiv = document.getElementById('stremthru_url_div');
    const authDiv = document.getElementById('stremthru_auth_div');
    const urlInput = document.getElementById('stremthru_url');
    const defaultUrl = 'https://stremthru.13377001.xyz/';

    if (isEnabled) {
        setElementDisplay('stremthru_url_div', 'block');
        if (authDiv) setElementDisplay('stremthru_auth_div', 'block');
        
        // Set default URL if empty or placeholder
        if (urlInput && (!urlInput.value || urlInput.value === urlInput.placeholder)) {
            urlInput.value = defaultUrl;
        }
    } else {
        setElementDisplay('stremthru_url_div', 'none');
        if (authDiv) setElementDisplay('stremthru_auth_div', 'none');
    }
}

function updateDebridDownloaderOptions() {
    const debridDownloaderOptions = document.getElementById('debridDownloaderOptions');
    if (!debridDownloaderOptions) return;

    debridDownloaderOptions.innerHTML = '';

    // --- Vérifier les services de débridage standard ---
    const rdEnabled = document.getElementById('debrid_rd')?.checked || document.getElementById('debrid_rd')?.disabled;
    const adEnabled = document.getElementById('debrid_ad')?.checked || document.getElementById('debrid_ad')?.disabled;
    const tbEnabled = document.getElementById('debrid_tb')?.checked || document.getElementById('debrid_tb')?.disabled;
    const pmEnabled = document.getElementById('debrid_pm')?.checked || document.getElementById('debrid_pm')?.disabled;
    const dlEnabled = document.getElementById('debrid_dl')?.checked || document.getElementById('debrid_dl')?.disabled;
    const edEnabled = document.getElementById('debrid_ed')?.checked || document.getElementById('debrid_ed')?.disabled;
    const ocEnabled = document.getElementById('debrid_oc')?.checked || document.getElementById('debrid_oc')?.disabled;
    const pkEnabled = document.getElementById('debrid_pk')?.checked || document.getElementById('debrid_pk')?.disabled;
    
    // --- Vérifier StremThru --- 
    const stremthruEnabledCheckbox = document.getElementById('stremthru_enabled');
    const stremthruEnabled = stremthruEnabledCheckbox ? stremthruEnabledCheckbox.checked : false;

    let firstOption = null;

    // --- Ajouter des options en fonction des services activés ---
    if (rdEnabled) {
        firstOption = addDebridDownloaderOption('Real-Debrid');
    }
    if (adEnabled) {
        // Utiliser l'opérateur ternaire pour une attribution plus propre
        firstOption = firstOption ? firstOption : addDebridDownloaderOption('AllDebrid'); 
        if (firstOption.value !== 'AllDebrid') addDebridDownloaderOption('AllDebrid');
    }
    if (tbEnabled) {
        firstOption = firstOption ? firstOption : addDebridDownloaderOption('TorBox');
        if (firstOption.value !== 'TorBox') addDebridDownloaderOption('TorBox');
    }
    if (pmEnabled) {
        firstOption = firstOption ? firstOption : addDebridDownloaderOption('Premiumize');
        if (firstOption.value !== 'Premiumize') addDebridDownloaderOption('Premiumize');
    }
    if (dlEnabled) {
        firstOption = firstOption ? firstOption : addDebridDownloaderOption('Debrid-Link');
        if (firstOption.value !== 'Debrid-Link') addDebridDownloaderOption('Debrid-Link');
    }
    if (edEnabled) {
        firstOption = firstOption ? firstOption : addDebridDownloaderOption('EasyDebrid');
        if (firstOption.value !== 'EasyDebrid') addDebridDownloaderOption('EasyDebrid');
    }
    if (ocEnabled) {
        firstOption = firstOption ? firstOption : addDebridDownloaderOption('Offcloud');
        if (firstOption.value !== 'Offcloud') addDebridDownloaderOption('Offcloud');
    }
    if (pkEnabled) {
        firstOption = firstOption ? firstOption : addDebridDownloaderOption('PikPak');
        if (firstOption.value !== 'PikPak') addDebridDownloaderOption('PikPak');
    }
    
    // --- Ajouter StremThru si activé --- 
    if (stremthruEnabled) {
        firstOption = firstOption ? firstOption : addDebridDownloaderOption('StremThru');
        if (firstOption.value !== 'StremThru') addDebridDownloaderOption('StremThru');
    }

    // Sélectionner la première option ajoutée par défaut si aucune n'est sélectionnée
    if (firstOption && !document.querySelector('input[name="debrid_downloader"]:checked')) {
        firstOption.checked = true;
    }
}

function addDebridDownloaderOption(serviceName) {
    const debridDownloaderOptions = document.getElementById('debridDownloaderOptions');
    const id = `debrid_downloader_${serviceName.toLowerCase().replace('-', '_')}`;

    const div = document.createElement('div');
    div.className = 'flex items-center';

    const input = document.createElement('input');
    input.type = 'radio';
    input.id = id;
    input.name = 'debrid_downloader';
    input.value = serviceName;
    input.className = 'h-4 w-4 border-gray-300 text-indigo-600 focus:ring-indigo-600';

    const label = document.createElement('label');
    label.htmlFor = id;
    label.className = 'ml-3 block text-sm font-medium text-white';
    label.textContent = serviceName;

    div.appendChild(input);
    div.appendChild(label);
    debridDownloaderOptions.appendChild(div);

    return input;
}


function updateProviderFields() {
    console.log("--- Running updateProviderFields ---"); // Debug start
    const stremthruEnabledCheckbox = document.getElementById('stremthru_enabled');
    let stremthruWasEnabled = stremthruEnabledCheckbox ? stremthruEnabledCheckbox.checked : false;
    let anyUnimplementedChecked = false;

    const serviceStates = {};
    const allDebrids = [...implementedDebrids, ...unimplementedDebrids];

    // Vérifier l'état des autres éléments de l'interface
    const utopeerChecked = document.getElementById('utopeer')?.checked || document.getElementById('utopeer')?.disabled;
    const torboxChecked = document.getElementById('debrid_tb')?.checked || document.getElementById('debrid_tb')?.disabled;
    const c411Checked = document.getElementById('c411')?.checked || document.getElementById('c411')?.disabled;
    const torr9Checked = document.getElementById('torr9')?.checked || document.getElementById('torr9')?.disabled;
    const lacaleChecked = document.getElementById('lacale')?.checked || document.getElementById('lacale')?.disabled;
    const generationfreeChecked = document.getElementById('generationfree')?.checked || document.getElementById('generationfree')?.disabled;
    const abnChecked = document.getElementById('abn')?.checked || document.getElementById('abn')?.disabled;
    const g3miniChecked = document.getElementById('g3mini')?.checked || document.getElementById('g3mini')?.disabled;
    const theoldschoolChecked = document.getElementById('theoldschool')?.checked || document.getElementById('theoldschool')?.disabled;

    // Afficher/masquer les champs spécifiques
    setElementDisplay('tb_debrid-fields', torboxChecked ? 'block' : 'none');
    setElementDisplay('c411-fields', c411Checked ? 'block' : 'none');
    setElementDisplay('torr9-fields', torr9Checked ? 'block' : 'none');
    setElementDisplay('lacale-fields', lacaleChecked ? 'block' : 'none');
    setElementDisplay('generationfree-fields', generationfreeChecked ? 'block' : 'none');
    setElementDisplay('abn-fields', abnChecked ? 'block' : 'none');
    setElementDisplay('g3mini-fields', g3miniChecked ? 'block' : 'none');
    setElementDisplay('theoldschool-fields', theoldschoolChecked ? 'block' : 'none');

    // Afficher/masquer les sélecteurs de phase de recherche
    setElementDisplay('c411-category-row', c411Checked ? 'block' : 'none');
    setElementDisplay('torr9-category-row', torr9Checked ? 'block' : 'none');
    setElementDisplay('lacale-category-row', lacaleChecked ? 'block' : 'none');
    setElementDisplay('generationfree-category-row', generationfreeChecked ? 'block' : 'none');
    setElementDisplay('abn-category-row', abnChecked ? 'block' : 'none');
    setElementDisplay('g3mini-category-row', g3miniChecked ? 'block' : 'none');
    setElementDisplay('theoldschool-category-row', theoldschoolChecked ? 'block' : 'none');

    // Traiter tous les débrideurs
    allDebrids.forEach(id => {
        const checkbox = document.getElementById(id);
        if (!checkbox) return;
        // isActive : service compté comme actif (compte unique serveur OU activé par l'utilisateur)
        // → utilisé pour la liste d'ordre des débrideurs et les stats globales
        const isActive = checkbox.checked || checkbox.disabled;
        // needsCredentials : l'utilisateur a activé le service lui-même (pas un compte unique serveur)
        // → si disabled, le serveur possède déjà le token, l'utilisateur n'a rien à configurer
        const needsCredentials = checkbox.checked && !checkbox.disabled;
        serviceStates[id] = isActive;

        // Déterminer l'ID du div de credentials correspondant
        let credDivId = '';
        switch (id) {
            case 'debrid_rd': credDivId = 'rd_token_info_div'; break;
            case 'debrid_ad': credDivId = 'ad_token_info_div'; break;
            case 'debrid_pm': credDivId = 'pm_token_info_div'; break;
            case 'debrid_tb': credDivId = 'tb_token_info_div'; break;
            case 'debrid_dl': credDivId = 'debridlink_api_key_div'; break;
            case 'debrid_ed': credDivId = 'easydebrid_api_key_div'; break;
            case 'debrid_oc': credDivId = 'offcloud_credentials_div'; break;
            case 'debrid_pk': credDivId = 'pikpak_credentials_div'; break;
        }

        // Afficher/masquer le div de credentials
        // On n'affiche que si l'utilisateur a activé le service lui-même (compte non-unique)
        if (credDivId) {
            setElementDisplay(credDivId, needsCredentials ? 'block' : 'none');
        }

        // Avertissement si un service nécessitant StremThru est coché sans que StremThru soit configuré
        if (unimplementedDebrids.includes(id) && isActive) {
            anyUnimplementedChecked = true;
        }
    });

    // Afficher un avertissement si des services StremThru-only sont activés sans StremThru configuré
    const stremthruWarningDiv = document.getElementById('stremthru_required_warning');
    if (stremthruWarningDiv) {
        const stremthruConfigured = stremthruEnabledCheckbox && stremthruEnabledCheckbox.checked;
        setElementDisplay('stremthru_required_warning', (anyUnimplementedChecked && !stremthruConfigured) ? 'block' : 'none');
    }

    // Afficher/masquer les champs StremThru selon l'état de la case
    if (stremthruEnabledCheckbox) {
        stremthruEnabledCheckbox.disabled = false; // Le checkbox reste toujours libre

        // Afficher/masquer les champs StremThru
        setElementDisplay('stremthru_url_div', stremthruEnabledCheckbox.checked ? 'block' : 'none');
        const authDiv = document.getElementById('stremthru_auth_div');
        if (authDiv) {
            setElementDisplay('stremthru_auth_div', stremthruEnabledCheckbox.checked ? 'block' : 'none');
        }
    }

    // Mettre à jour la visibilité des champs StremThru si l'état a changé
    if (stremthruEnabledCheckbox && stremthruEnabledCheckbox.checked !== stremthruWasEnabled) {
        toggleStremThruFields();
    }

    // Gérer l'ordre des débrideurs
    const debridOrderCheckbox = document.getElementById('debrid_order');
    const debridOrderList = document.getElementById('debridOrderList');

    if (debridOrderCheckbox && debridOrderList) {
        // Vérifier si au moins un débrideur est activé
        const anyDebridEnabled = Object.values(serviceStates).some(state => state);

        debridOrderCheckbox.disabled = !anyDebridEnabled;
        
        if (!anyDebridEnabled) {
            debridOrderCheckbox.checked = false;
        }

        debridOrderList.classList.toggle('hidden', !(anyDebridEnabled && debridOrderCheckbox.checked));
    }

    // Mettre à jour les listes et options
    updateDebridOrderList();
    updateDebridDownloaderOptions();
    ensureDebridConsistency();
    console.log("--- Finished updateProviderFields ---"); // Debug end
}

function ensureDebridConsistency() {
    // Récupérer l'état de tous les débrideurs
    const serviceStates = {};
    const allDebrids = [...implementedDebrids, ...unimplementedDebrids];
    let anyDebridChecked = false;
    let anyUnimplementedChecked = false;

    allDebrids.forEach(id => {
        const checkbox = document.getElementById(id);
        if (!checkbox) return;
        const isChecked = checkbox.checked;
        serviceStates[id] = isChecked;
        if (isChecked) {
            anyDebridChecked = true;
            if (unimplementedDebrids.includes(id)) {
                anyUnimplementedChecked = true;
            }
        }
    });

    // Gérer l'état de la case à cocher d'ordre des débrideurs
    const debridOrderCheckbox = document.getElementById('debrid_order');
    const debridOrderList = document.getElementById('debridOrderList');
    
    if (debridOrderCheckbox && debridOrderList) {
        if (!anyDebridChecked) {
            debridOrderCheckbox.checked = false;
            debridOrderList.classList.add('hidden');
        }

        if (debridOrderCheckbox.checked && !anyDebridChecked) {
            debridOrderCheckbox.checked = false;
        }
    }

    // StremThru : le checkbox reste libre, pas de forçage
    const stremthruEnabledCheckbox = document.getElementById('stremthru_enabled');
    if (stremthruEnabledCheckbox) {
        stremthruEnabledCheckbox.disabled = false;
    }

    updateDebridDownloaderOptions();
}

function loadData() {
    let decodedData = {};

    // 1. Priorité : config stockée en localStorage (populée par getLink après chiffrement)
    const stored = localStorage.getItem('streamfusion_config');
    if (stored) {
        try {
            decodedData = JSON.parse(stored);
        } catch (e) {
            console.warn("Config localStorage invalide, suppression.");
            localStorage.removeItem('streamfusion_config');
        }
    }

    // 2. Fallback : anciennes URLs base64 (rétrocompatibilité avec les bookmarks existants)
    if (Object.keys(decodedData).length === 0) {
        const currentUrl = window.location.href;
        const data = currentUrl.match(/\/([^\/]+)\/configure$/);
        if (data && data[1]) {
            try {
                decodedData = JSON.parse(atob(data[1]));
            } catch (error) {
                console.warn("Pas de données base64 valides dans l'URL, valeurs par défaut utilisées.");
            }
        }
    }

    function setElementValue(id, value, defaultValue) {
        const element = document.getElementById(id);
        if (element) {
            if (element.type === 'radio' || element.type === 'checkbox') {
                element.checked = (value !== undefined) ? value : defaultValue;
            } else {
                element.value = value || defaultValue || '';
            }
        }
    }

    const defaultConfig = {
        jackett: false,
        zilean: true,
        utopeer: true,
        maxSize: '150',
        resultsPerQuality: '10',
        maxResults: '30',
        minCachedResults: '10',
        torrenting: false,
        ctg_yggtorrent: false,
        ctg_utopeer: false,
        metadataProvider: 'tmdb',
        sort: 'quality',
        exclusion: ['cam'],
        languages: ['fr', 'multi'],
        debrid_rd: false,
        debrid_ad: false,
        debrid_tb: false,
        debrid_pm: false,
        tb_usenet: false,
        tb_search: false,
        debrid_order: false,
        c411: false,
        torr9: false,
        lacale: false,
        generationfree: false,
        abn: false,
        g3mini: false,
        theoldschool: false,
        utopeerPriority: true,
        rdMinCachedBeforeCheck: 3,
        minPostgresResults: 20,
        postgresMaxAgeDays: 7,
    };

    Object.keys(defaultConfig).forEach(key => {
        const value = decodedData[key] !== undefined ? decodedData[key] : defaultConfig[key];
        if (key === 'metadataProvider') {
            setElementValue('tmdb', value === 'tmdb', true);
            setElementValue('cinemeta', value === 'cinemeta', false);
        } else if (key === 'sort') {
            sorts.forEach(sort => {
                setElementValue(sort, value === sort, sort === defaultConfig.sort);
            });
        } else if (key === 'exclusion') {
            qualityExclusions.forEach(quality => {
                setElementValue(quality, value.includes(quality), defaultConfig.exclusion.includes(quality));
            });
        } else if (key === 'languages') {
            languages.forEach(language => {
                setElementValue(language, value.includes(language), defaultConfig.languages.includes(language));
            });
        } else {
            setElementValue(key, value, defaultConfig[key]);
        }
    });

    const serviceArray = decodedData.service || [];
    setElementValue('debrid_rd', serviceArray.includes('Real-Debrid'), defaultConfig.debrid_rd);
    setElementValue('debrid_ad', serviceArray.includes('AllDebrid'), defaultConfig.debrid_ad);
    setElementValue('debrid_tb', serviceArray.includes('TorBox'), defaultConfig.debrid_tb);
    setElementValue('debrid_pm', serviceArray.includes('Premiumize'), defaultConfig.debrid_pm);
    setElementValue('debrid_order', serviceArray.length > 0, defaultConfig.debrid_order);
    
    setElementValue('ctg_yggtorrent', decodedData.yggtorrentCtg, defaultConfig.ctg_yggtorrent);
    setElementValue('ctg_utopeer', decodedData.utopeerCtg, defaultConfig.ctg_utopeer);
    
    setElementValue('rd_token_info', decodedData.RDToken, '');
    setElementValue('ad_token_info', decodedData.ADToken, '');
    setElementValue('tb_token_info', decodedData.TBToken, '');
    setElementValue('pm_token_info', decodedData.PMToken, '');
    setElementValue('c411ApiKey', decodedData.c411ApiKey, '');
    setElementValue('c411Passkey', decodedData.c411Passkey, '');
    setElementValue('torr9ApiKey', decodedData.torr9ApiKey, '');
    setElementValue('lacaleApiKey', decodedData.lacaleApiKey, '');
    setElementValue('generationfreeApiKey', decodedData.generationfreeApiKey, '');
    setElementValue('generationfreePasskey', decodedData.generationfreePasskey, '');
    setElementValue('abnApiKey', decodedData.abnApiKey, '');
    setElementValue('abnPasskey', decodedData.abnPasskey, '');
    setElementValue('g3miniApiKey', decodedData.g3miniApiKey, '');
    setElementValue('g3miniPasskey', decodedData.g3miniPasskey, '');
    setElementValue('theoldschoolApiKey', decodedData.theoldschoolApiKey, '');
    setElementValue('theoldschoolPasskey', decodedData.theoldschoolPasskey, '');
    setElementValue('ApiKey', decodedData.apiKey, '');
    setElementValue('exclusion-keywords', (decodedData.exclusionKeywords || []).join(', '), '');
    
    setElementValue('tb_usenet', decodedData.TBUsenet, defaultConfig.tb_usenet);
    setElementValue('tb_search', decodedData.TBSearch, defaultConfig.tb_search);

    setElementValue('stremthru_enabled', decodedData.stremthru, false);
    setElementValue('stremthru_url', decodedData.stremthruUrl, 'https://stremthru.13377001.xyz/');
    setElementValue('debridlink_api_key', decodedData.DLToken, '');
    setElementValue('easydebrid_api_key', decodedData.EDToken, '');
    setElementValue('offcloud_credentials', decodedData.OCCredentials, '');
    setElementValue('pikpak_credentials', decodedData.PPCredentials, '');

    handleUniqueAccounts();
    updateProviderFields();

    // Restaurer les catégories de phase de recherche par indexeur
    const _defaultCats = {
        c411: 'priority_private', torr9: 'priority_private',
        lacale: 'intermediary_private', generationfree: 'intermediary_private',
        g3mini: 'intermediary_private', theoldschool: 'intermediary_private',
        abn: 'fallback_private',
    };
    const _savedCats = decodedData.indexerCategories || {};
    for (const [key, defaultCat] of Object.entries(_defaultCats)) {
        const select = document.getElementById(key + 'Category');
        if (select) select.value = _savedCats[key] || defaultCat;
    }

    const debridDownloader = decodedData.debridDownloader;
    if (debridDownloader) {
        const radioButton = document.querySelector(`input[name="debrid_downloader"][value="${debridDownloader}"]`);
        if (radioButton) {
            radioButton.checked = true;
        }
    }

    updateDebridDownloaderOptions();
    updateDebridOrderList();
    ensureDebridConsistency();
    restoreAuthStates();
}

// Restaure l'état visuel "connecté" des boutons OAuth quand un token est déjà présent
// (chargement depuis URL ou retour sur la page de config avec un lien existant).
function restoreAuthStates() {
    // Real-Debrid
    const rdTokenEl = document.getElementById('rd_token_info');
    const rdBtn     = document.getElementById('rd-auth-button');
    if (rdTokenEl && rdBtn && rdTokenEl.value && rdTokenEl.value.trim()) {
        applyConnectedState(
            rdBtn,
            rdTokenEl,
            "S'authentifier avec Real-Debrid",
            '✓ Déjà connecté à Real-Debrid',
            'rd-reauth-btn'
        );
    }

    // AllDebrid
    const adTokenEl = document.getElementById('ad_token_info');
    const adBtn     = document.getElementById('ad-auth-button');
    if (adTokenEl && adBtn && adTokenEl.value && adTokenEl.value.trim()) {
        applyConnectedState(
            adBtn,
            adTokenEl,
            "S'authentifier avec AllDebrid",
            '✓ Déjà connecté à AllDebrid',
            'ad-reauth-btn'
        );
    }
}

// Applique l'état visuel "connecté" sur un bouton OAuth et ajoute un lien "Reconnecter"
function applyConnectedState(btn, tokenEl, originalText, connectedText, reauthId) {
    btn.disabled    = true;
    btn.textContent = connectedText;
    btn.classList.add('opacity-50', 'cursor-not-allowed');

    if (document.getElementById(reauthId)) return; // déjà injecté

    const reauthBtn = document.createElement('button');
    reauthBtn.type      = 'button';
    reauthBtn.id        = reauthId;
    reauthBtn.textContent = 'Reconnecter';
    reauthBtn.className = 'btn-oauth';
    reauthBtn.style.cssText = 'font-size:0.8rem;padding:5px 12px;opacity:0.65;margin-left:8px;';
    reauthBtn.onclick = function () {
        tokenEl.value = '';
        btn.disabled  = false;
        btn.textContent = originalText;
        btn.classList.remove('opacity-50', 'cursor-not-allowed');
        reauthBtn.remove();
    };
    btn.parentNode.insertBefore(reauthBtn, btn.nextSibling);
}

// Fonction pour valider l'API key
function validateApiKey(apiKey) {
    // Référence à l'élément d'erreur
    const apiKeyErrorElement = document.getElementById('apiKeyError');
    
    // Si aucune API key n'est fournie
    if (!apiKey || apiKey.trim() === '') {
        if (apiKeyErrorElement) {
            apiKeyErrorElement.classList.remove('hidden');
        }
        alert('Veuillez fournir une API Key Stream Fusion.');
        return false;
    }
    
    // Vérification du format UUID v4
    const isValidFormat = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(apiKey);
    
    if (!isValidFormat) {
        if (apiKeyErrorElement) {
            apiKeyErrorElement.classList.remove('hidden');
        }
        alert('API Key invalide.');
        return false;
    }
    
    // Si l'API key est valide, masquer le message d'erreur
    if (apiKeyErrorElement) {
        apiKeyErrorElement.classList.add('hidden');
    }
    
    return true;
}

async function getLink(method) {
    const apiKey = document.getElementById('ApiKey').value;
    
    // Vérifier l'API key en premier
    if (!validateApiKey(apiKey)) {
        return false;
    }
    
    const data = {
        addonHost: new URL(window.location.href).origin,
        apiKey: apiKey,
        service: [],
        RDToken: document.getElementById('rd_token_info')?.value,
        ADToken: document.getElementById('ad_token_info')?.value,
        TBToken: document.getElementById('tb_token_info')?.value,
        PMToken: document.getElementById('pm_token_info')?.value,
        TBUsenet: document.getElementById('tb_usenet')?.checked,
        TBSearch: document.getElementById('tb_search')?.checked,
        c411: document.getElementById('c411')?.checked || document.getElementById('c411')?.disabled || false,
        torr9: document.getElementById('torr9')?.checked || document.getElementById('torr9')?.disabled || false,
        lacale: document.getElementById('lacale')?.checked || document.getElementById('lacale')?.disabled || false,
        c411ApiKey: document.getElementById('c411ApiKey')?.value || '',
        c411Passkey: document.getElementById('c411Passkey')?.value || '',
        torr9ApiKey: document.getElementById('torr9ApiKey')?.value || '',
        lacaleApiKey: document.getElementById('lacaleApiKey')?.value || '',
        generationfree: document.getElementById('generationfree')?.checked || document.getElementById('generationfree')?.disabled || false,
        generationfreeApiKey: document.getElementById('generationfreeApiKey')?.value || '',
        generationfreePasskey: document.getElementById('generationfreePasskey')?.value || '',
        abn: document.getElementById('abn')?.checked || document.getElementById('abn')?.disabled || false,
        abnApiKey: document.getElementById('abnApiKey')?.value || '',
        abnPasskey: document.getElementById('abnPasskey')?.value || '',
        g3mini: document.getElementById('g3mini')?.checked || document.getElementById('g3mini')?.disabled || false,
        g3miniApiKey: document.getElementById('g3miniApiKey')?.value || '',
        g3miniPasskey: document.getElementById('g3miniPasskey')?.value || '',
        theoldschool: document.getElementById('theoldschool')?.checked || document.getElementById('theoldschool')?.disabled || false,
        theoldschoolApiKey: document.getElementById('theoldschoolApiKey')?.value || '',
        theoldschoolPasskey: document.getElementById('theoldschoolPasskey')?.value || '',
        maxSize: parseInt(document.getElementById('maxSize').value) || 16,
        exclusionKeywords: document.getElementById('exclusion-keywords').value.split(',').map(keyword => keyword.trim()).filter(keyword => keyword !== ''),
        languages: languages.filter(lang => document.getElementById(lang).checked),
        sort: sorts.find(sort => document.getElementById(sort).checked),
        resultsPerQuality: parseInt(document.getElementById('resultsPerQuality').value) || 5,
        maxResults: parseInt(document.getElementById('maxResults').value) || 5,
        minCachedResults: parseInt(document.getElementById('minCachedResults').value) || 5,
        rdMinCachedBeforeCheck: parseInt(document.getElementById('rdMinCachedBeforeCheck').value) ?? 3,
        minPostgresResults: parseInt(document.getElementById('minPostgresResults').value) ?? 5,
        postgresMaxAgeDays: parseInt(document.getElementById('postgresMaxAgeDays').value) ?? 7,
        exclusion: qualityExclusions.filter(quality => document.getElementById(quality).checked),
        jackett: document.getElementById('jackett')?.checked,
        zilean: document.getElementById('zilean')?.checked,
        utopeer: document.getElementById('utopeer')?.checked,
        utopeerPriority: document.getElementById('utopeerPriority')?.checked !== false,
        indexerCategories: (() => {
            const cats = {};
            const _privCats = {
                c411: 'priority_private', torr9: 'priority_private',
                lacale: 'intermediary_private', generationfree: 'intermediary_private',
                g3mini: 'intermediary_private', theoldschool: 'intermediary_private',
                abn: 'fallback_private',
            };
            for (const [key, defaultCat] of Object.entries(_privCats)) {
                const cb = document.getElementById(key);
                const enabled = cb?.checked || cb?.disabled || false;
                const select = document.getElementById(key + 'Category');
                cats[key] = enabled ? (select?.value || defaultCat) : 'disabled';
            }
            // Indexeurs à catégorie fixe
            const yggEl = document.getElementById('utopeer');
            cats.utopeer = (yggEl?.checked || yggEl?.disabled) ? 'public' : 'disabled';
            cats.zilean = document.getElementById('zilean')?.checked ? 'fallback_private' : 'disabled';
            cats.jackett = document.getElementById('jackett')?.checked ? 'fallback_private' : 'disabled';
            return cats;
        })(),
        yggtorrentCtg: document.getElementById('ctg_yggtorrent')?.checked,
        utopeerCtg: document.getElementById('ctg_utopeer')?.checked,
        torrenting: false,
        debrid: false,
        metadataProvider: document.getElementById('tmdb').checked ? 'tmdb' : 'cinemeta',
        debridDownloader: document.querySelector('input[name="debrid_downloader"]:checked')?.value,
        // StremThru configuration
        stremthru: document.getElementById('stremthru_enabled')?.checked || false,
        stremthruUrl: document.getElementById('stremthru_url')?.value || 'https://stremthru.13377001.xyz',
        // Nouveaux débrideurs
        DLToken: document.getElementById('debridlink_api_key')?.value || '',
        EDToken: document.getElementById('easydebrid_api_key')?.value || '',
        OCCredentials: document.getElementById('offcloud_credentials')?.value || '',
        PPCredentials: document.getElementById('pikpak_credentials')?.value || ''
    };

    data.service = Array.from(document.getElementById('debridOrderList').children).map(li => li.dataset.serviceName);
    data.debrid = data.service.length > 0;

    const missingRequiredFields = [];

    if (data.service.includes('Real-Debrid') && document.getElementById('rd_token_info') && !data.RDToken) missingRequiredFields.push("Real-Debrid Account Connection");
    if (data.service.includes('AllDebrid') && document.getElementById('ad_token_info') && !data.ADToken) missingRequiredFields.push("AllDebrid Account Connection");
    if (data.service.includes('TorBox') && document.getElementById('tb_token_info') && !data.TBToken) missingRequiredFields.push("TorBox Account Connection");
    if (data.service.includes('Premiumize') && document.getElementById('pm_token_info') && !data.PMToken) missingRequiredFields.push("Premiumize Account Connection");
    if (data.service.includes('Debrid-Link') && document.getElementById('debridlink_api_key') && !data.DLToken) missingRequiredFields.push("Debrid-Link API Key");
    if (data.service.includes('EasyDebrid') && document.getElementById('easydebrid_api_key') && !data.EDToken) missingRequiredFields.push("EasyDebrid API Key");
    if (data.service.includes('Offcloud') && document.getElementById('offcloud_credentials') && !data.OCCredentials) missingRequiredFields.push("Offcloud Credentials");
    if (data.service.includes('PikPak') && document.getElementById('pikpak_credentials') && !data.PPCredentials) missingRequiredFields.push("PikPak Credentials");
    if (data.languages.length === 0) missingRequiredFields.push("Languages");
    if (data.c411 && document.getElementById('c411ApiKey') && !data.c411ApiKey) missingRequiredFields.push("C411 API Key");
    if (data.torr9 && document.getElementById('torr9ApiKey') && !data.torr9ApiKey) missingRequiredFields.push("Torr9 API Key");
    if (data.lacale && document.getElementById('lacaleApiKey') && !data.lacaleApiKey) missingRequiredFields.push("LaCale API Key");
    if (data.generationfree && document.getElementById('generationfreeApiKey') && !data.generationfreeApiKey) missingRequiredFields.push("Generation Free API Key");
    if (data.abn && document.getElementById('abnApiKey') && !data.abnApiKey) missingRequiredFields.push("ABN API Key");
    if (data.g3mini && document.getElementById('g3miniApiKey') && !data.g3miniApiKey) missingRequiredFields.push("G3MINI API Key");
    if (data.theoldschool && document.getElementById('theoldschoolApiKey') && !data.theoldschoolApiKey) missingRequiredFields.push("TheOldSchool API Key");
    if (data.stremthru && !data.stremthruUrl) missingRequiredFields.push("StremThru URL");

    if (missingRequiredFields.length > 0) {
        alert(`Please fill all required fields: ${missingRequiredFields.join(", ")}`);
        return false;
    }



    // Appel serveur pour chiffrement Fernet (protégé par token CSRF)
    let token;
    try {
        const csrfToken = window.__CSRF_TOKEN__ || '';
        const response = await fetch('/api/config/encode', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'X-CSRF-Token': csrfToken
            },
            body: JSON.stringify({ config: data })
        });
        if (!response.ok) {
            throw new Error(`Erreur serveur: ${response.status}`);
        }
        const result = await response.json();
        token = result.token;
        if (!result.encrypted) {
            console.warn('CONFIG_SECRET_KEY absente sur le serveur — config encodée en Base64 seulement (non chiffrée).');
        }
    } catch (err) {
        alert(`Échec de l'encodage de la configuration: ${err.message}`);
        return false;
    }

    // Stocker la config brute en localStorage pour repopuler /{config}/configure
    localStorage.setItem('streamfusion_config', JSON.stringify(data));

    const manifestUrl = window.location.protocol + '//' + window.location.host + '/' + token + '/manifest.json';

    if (method === 'link') {
        window.open('stremio://' + window.location.host + '/' + token + '/manifest.json', "_blank");
    } else if (method === 'copy') {
        await navigator.clipboard.writeText(manifestUrl);
    }

    return manifestUrl;
}

let showLanguageCheckBoxes = true;
function showCheckboxes() {
    let checkboxes = document.getElementById("languageCheckBoxes");
    checkboxes.style.display = showLanguageCheckBoxes ? "block" : "none";
    showLanguageCheckBoxes = !showLanguageCheckBoxes;
}

// Fonction pour valider l'API key sans afficher d'alert
function validateApiKeyWithoutAlert(apiKey) {
    const apiKeyErrorElement = document.getElementById('apiKeyError');
    
    // Si aucune API key n'est fournie
    if (!apiKey || apiKey.trim() === '') {
        if (apiKeyErrorElement) {
            apiKeyErrorElement.classList.remove('hidden');
        }
        return false;
    }
    
    // Vérification du format UUID v4
    const isValidFormat = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(apiKey);
    
    if (!isValidFormat) {
        if (apiKeyErrorElement) {
            apiKeyErrorElement.classList.remove('hidden');
        }
        return false;
    }
    
    // Si l'API key est valide, masquer le message d'erreur
    if (apiKeyErrorElement) {
        apiKeyErrorElement.classList.add('hidden');
    }
    
    return true;
}
