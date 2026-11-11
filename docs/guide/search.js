// Rusternetes Documentation Search
// Pure vanilla JS — no dependencies
(function() {
  'use strict';

  // Search index: populated from page data attributes
  var INDEX = [
    // Getting Started
    { title: 'Quick Start', section: 'Getting Started', url: 'quickstart.html', keywords: 'quick start install 5 minutes docker compose deploy cluster' },
    { title: 'Installation', section: 'Getting Started', url: 'installation.html', keywords: 'install build source cargo rust prerequisites docker podman' },
    // Deployment
    { title: 'Deployment Overview', section: 'Deployment', url: 'deployment.html', keywords: 'deploy modes etcd sqlite all-in-one comparison choose' },
    { title: 'All-in-One Binary', section: 'Deployment', url: 'all-in-one.html', keywords: 'single binary sqlite embedded tokio process edge iot ci' },
    { title: 'Docker Cluster (etcd)', section: 'Deployment', url: 'docker-cluster.html', keywords: 'docker compose etcd cluster multi-node production' },
    { title: 'Docker Cluster (SQLite)', section: 'Deployment', url: 'sqlite-cluster.html', keywords: 'docker compose sqlite rhino lightweight no-etcd' },
    { title: 'High Availability', section: 'Deployment', url: 'high-availability.html', keywords: 'ha multi-master leader election etcd cluster 3-node' },
    // Cluster Bootstrap
    { title: 'Bootstrap', section: 'Cluster Setup', url: 'bootstrap.html', keywords: 'bootstrap coredns dns service account tokens certificates' },
    // Configuration
    { title: 'API Server', section: 'Configuration', url: 'api-server-config.html', keywords: 'api server config tls jwt rbac auth bind address flags' },
    { title: 'Kubelet', section: 'Configuration', url: 'kubelet-config.html', keywords: 'kubelet config node docker volumes probes sync interval' },
    { title: 'Scheduler', section: 'Configuration', url: 'scheduler-config.html', keywords: 'scheduler config interval plugins leader election metrics' },
    { title: 'Controller Manager', section: 'Configuration', url: 'controller-manager-config.html', keywords: 'controller manager config sync interval cloud provider controllers' },
    { title: 'Kube-Proxy', section: 'Configuration', url: 'kube-proxy-config.html', keywords: 'kube proxy config iptables service routing node clusterip nodeport' },
    { title: 'Storage Backends', section: 'Configuration', url: 'storage-config.html', keywords: 'storage backend etcd sqlite rhino memory config database' },
    // Features
    { title: 'Workloads', section: 'Features', url: 'workloads.html', keywords: 'deployment replicaset statefulset daemonset job cronjob pod workload' },
    { title: 'Networking', section: 'Features', url: 'networking.html', keywords: 'service endpoint dns coredns network policy ingress clusterip nodeport loadbalancer cni calico cilium flannel weave multus bridge overlay pod cidr' },
    { title: 'Storage & Volumes', section: 'Features', url: 'storage-volumes.html', keywords: 'pvc pv volume emptydir hostpath secret configmap projected nfs csi driver storageclass provisioner dynamic provisioning snapshot expansion default standard' },
    { title: 'Security', section: 'Features', url: 'security.html', keywords: 'rbac tls webhook admission serviceaccount token security context secrets' },
    { title: 'Custom Resources (CRDs)', section: 'Features', url: 'crds.html', keywords: 'crd custom resource definition schema watch status scale validation' },
    { title: 'Scheduling & Autoscaling', section: 'Features', url: 'scheduling.html', keywords: 'scheduler affinity taint toleration priority preemption hpa vpa pdb' },
    { title: 'Container Runtime', section: 'Features', url: 'containers.html', keywords: 'container probe liveness readiness startup lifecycle hook exec attach logs' },
    { title: 'kubectl Reference', section: 'Features', url: 'kubectl.html', keywords: 'kubectl cli command get create apply delete logs exec scale rollout' },
    // Operations
    { title: 'Monitoring', section: 'Operations', url: 'monitoring.html', keywords: 'metrics prometheus tracing health healthz livez readyz' },
    { title: 'Troubleshooting', section: 'Operations', url: 'troubleshooting.html', keywords: 'troubleshoot debug error fix connection refused watch dns pending' },
    // Reference
    { title: 'API Reference', section: 'Reference', url: 'api-reference.html', keywords: 'api endpoint resource type group version route handler' },
    { title: 'Conformance', section: 'Reference', url: 'conformance.html', keywords: 'conformance test sonobuoy e2e pass fail kubernetes' },
    // Console & Auth
    { title: 'Web Console', section: 'Features', url: 'console.html', keywords: 'console dashboard topology visualization metrics logs deploy scale restart create resource explorer cni networking storage rbac events fleet multi-cluster' },
    { title: 'Authentication', section: 'Operations', url: 'authentication.html', keywords: 'authentication authorization rbac jwt token mTLS client certificate skip-auth security serviceaccount' },
  ];

  var input, resultsContainer;

  function init() {
    input = document.querySelector('.doc-search-input');
    resultsContainer = document.querySelector('.doc-search-results');
    if (!input || !resultsContainer) return;

    input.addEventListener('input', onInput);
    input.addEventListener('focus', function() { if (input.value) onInput(); });
    input.addEventListener('keydown', onKeydown);

    // Press / to focus search
    document.addEventListener('keydown', function(e) {
      if (e.key === '/' && document.activeElement !== input) {
        e.preventDefault();
        input.focus();
      }
      if (e.key === 'Escape') {
        resultsContainer.classList.remove('visible');
        input.blur();
      }
    });

    // Click outside to close
    document.addEventListener('click', function(e) {
      if (!e.target.closest('.doc-search')) {
        resultsContainer.classList.remove('visible');
      }
    });
  }

  function onInput() {
    var q = input.value.trim().toLowerCase();
    if (!q) {
      resultsContainer.classList.remove('visible');
      return;
    }
    var results = search(q);
    renderResults(results);
  }

  function search(query) {
    var terms = query.split(/\s+/);
    var scored = [];
    for (var i = 0; i < INDEX.length; i++) {
      var entry = INDEX[i];
      var text = (entry.title + ' ' + entry.section + ' ' + entry.keywords).toLowerCase();
      var score = 0;
      var allMatch = true;
      for (var j = 0; j < terms.length; j++) {
        if (text.indexOf(terms[j]) === -1) {
          allMatch = false;
          break;
        }
        // Title matches score higher
        if (entry.title.toLowerCase().indexOf(terms[j]) !== -1) score += 10;
        else if (entry.section.toLowerCase().indexOf(terms[j]) !== -1) score += 5;
        else score += 1;
      }
      if (allMatch && score > 0) scored.push({ entry: entry, score: score });
    }
    scored.sort(function(a, b) { return b.score - a.score; });
    return scored.slice(0, 8);
  }

  function renderResults(results) {
    if (!results.length) {
      resultsContainer.innerHTML = '<div class="search-result" style="opacity:0.5">No results found</div>';
      resultsContainer.classList.add('visible');
      return;
    }
    var html = '';
    for (var i = 0; i < results.length; i++) {
      var r = results[i].entry;
      html += '<a class="search-result" href="' + r.url + '">' +
        r.title + ' <span class="search-result-section">' + r.section + '</span></a>';
    }
    resultsContainer.innerHTML = html;
    resultsContainer.classList.add('visible');
  }

  var selectedIdx = -1;
  function onKeydown(e) {
    var items = resultsContainer.querySelectorAll('.search-result[href]');
    if (e.key === 'ArrowDown') {
      e.preventDefault();
      selectedIdx = Math.min(selectedIdx + 1, items.length - 1);
      updateSelected(items);
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      selectedIdx = Math.max(selectedIdx - 1, 0);
      updateSelected(items);
    } else if (e.key === 'Enter' && selectedIdx >= 0 && items[selectedIdx]) {
      e.preventDefault();
      window.location.href = items[selectedIdx].href;
    }
  }

  function updateSelected(items) {
    for (var i = 0; i < items.length; i++) {
      items[i].classList.toggle('selected', i === selectedIdx);
    }
  }

  // Sidebar toggle for mobile
  var toggle = document.querySelector('.sidebar-toggle');
  var sidebar = document.querySelector('.doc-sidebar');
  if (toggle && sidebar) {
    toggle.addEventListener('click', function() {
      sidebar.classList.toggle('open');
    });
    var overlay = document.querySelector('.doc-overlay');
    if (overlay) {
      overlay.addEventListener('click', function() {
        sidebar.classList.remove('open');
      });
    }
  }

  // Collapsible sidebar sections
  var headings = document.querySelectorAll('.sidebar-heading');
  for (var i = 0; i < headings.length; i++) {
    headings[i].addEventListener('click', function() {
      this.parentElement.classList.toggle('collapsed');
    });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
