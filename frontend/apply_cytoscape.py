import re

file_path = r'c:\Users\Sanchit\Desktop\Work\College\DSA\CP\frontend\index.html'

with open(file_path, 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Update CASCADE_PALETTES to use clean high-precision palette
old_palettes = '''    const CASCADE_PALETTES = [
      { primary: '#00f2fe', pulse: '#38bdf8', edge: '#00f2fe' },
      { primary: '#10b981', pulse: '#34d399', edge: '#10b981' },
      { primary: '#f59e0b', pulse: '#fbbf24', edge: '#f59e0b' },
      { primary: '#8b5cf6', pulse: '#a78bfa', edge: '#8b5cf6' },
      { primary: '#ec4899', pulse: '#f472b6', edge: '#ec4899' }
    ];'''

new_palettes = '''    const CASCADE_PALETTES = [
      { primary: '#06b6d4', pulse: '#22d3ee', edge: '#06b6d4' },
      { primary: '#10b981', pulse: '#34d399', edge: '#10b981' },
      { primary: '#f59e0b', pulse: '#fbbf24', edge: '#f59e0b' },
      { primary: '#38bdf8', pulse: '#7dd3fc', edge: '#38bdf8' },
      { primary: '#a1a1aa', pulse: '#e4e4e7', edge: '#a1a1aa' }
    ];'''

if old_palettes in content:
    content = content.replace(old_palettes, new_palettes)
    print('CASCADE_PALETTES updated.')
else:
    print('Warning: old palettes not found')

# 2. Update Cytoscape instance configuration & event handlers
cy_start = content.find('const cy = cytoscape({')
cy_end = content.find('\n      // Handlers\n      const handleSelectCandidate =')

new_cy = '''const cy = cytoscape({
          container: document.getElementById('cy'),
          style: [
            {
              selector: 'node',
              style: {
                'label': 'data(displayLabel)',
                'color': '#a1a1aa',
                'font-family': 'Inter, sans-serif',
                'font-size': '10px',
                'font-weight': 500,
                'text-valign': 'bottom',
                'text-margin-y': '5px',
                'text-background-color': '#09090b',
                'text-background-opacity': 0.85,
                'text-background-padding': '2px',
                'text-background-shape': 'roundrectangle',
                'background-color': '#3f3f46',
                'width': 'data(size)',
                'height': 'data(size)',
                'border-width': 1.5,
                'border-color': '#09090b'
              }
            },
            {
              selector: 'node[type = "influencer"]',
              style: {
                'background-color': '#06b6d4',
                'border-color': '#09090b',
                'border-width': 1.5,
                'color': '#f4f4f5',
                'font-weight': 600,
                'font-size': '10.5px'
              }
            },
            {
              selector: 'node[type = "bot"]',
              style: {
                'background-color': '#ef4444',
                'border-color': '#09090b',
                'border-width': 1.5,
                'label': 'data(displayLabel)',
                'color': '#fca5a5'
              }
            },
            {
              selector: 'edge',
              style: {
                'width': 1.0,
                'line-color': 'rgba(148, 163, 184, 0.15)',
                'target-arrow-color': 'rgba(148, 163, 184, 0.2)',
                'target-arrow-shape': 'triangle',
                'curve-style': 'bezier',
                'arrow-scale': 0.7,
                'opacity': 1.0
              }
            },
            {
              selector: 'edge[type = "collab_edge"]',
              style: {
                'width': 1.5,
                'line-color': 'rgba(6, 182, 212, 0.35)',
                'target-arrow-color': 'rgba(6, 182, 212, 0.5)',
                'target-arrow-shape': 'triangle',
                'curve-style': 'bezier',
                'arrow-scale': 0.85
              }
            },
            {
              selector: 'edge[type = "tw_hub_edge"]',
              style: {
                'width': 1.5,
                'line-color': 'rgba(6, 182, 212, 0.35)',
                'target-arrow-color': 'rgba(6, 182, 212, 0.5)',
                'target-arrow-shape': 'triangle',
                'curve-style': 'bezier',
                'arrow-scale': 0.85
              }
            },
            {
              selector: 'edge[type = "tw_edge"]',
              style: {
                'width': 1.0,
                'line-color': 'rgba(148, 163, 184, 0.15)',
                'target-arrow-color': 'rgba(148, 163, 184, 0.2)',
                'target-arrow-shape': 'triangle',
                'curve-style': 'bezier',
                'arrow-scale': 0.7
              }
            },
            {
              selector: 'edge[type = "bot_edge"]',
              style: {
                'line-color': 'rgba(239, 68, 68, 0.45)',
                'target-arrow-color': 'rgba(239, 68, 68, 0.6)',
                'width': 1.5,
                'line-style': 'dashed'
              }
            },
            {
              selector: 'node.highlighted',
              style: {
                'background-color': '#06b6d4',
                'border-color': '#f4f4f5',
                'border-width': 2.5
              }
            },
            {
              selector: 'edge.highlighted',
              style: {
                'line-color': '#06b6d4',
                'target-arrow-color': '#06b6d4',
                'width': 2.0,
                'opacity': 0.8,
                'z-index': 99
              }
            }
          ],
          layout: {
            name: 'cose', animate: true, idealEdgeLength: 120, nodeOverlap: 25
          }
        });

        // Dynamic label density: Top 15 by PageRank or when zoomed in
        const updateLabelVisibility = () => {
          if (!cy) return;
          const currentZoom = cy.zoom();
          const nodes = cy.nodes();
          const sorted = [...nodes].sort((a, b) => (parseFloat(b.data('pageRank')) || 0) - (parseFloat(a.data('pageRank')) || 0));
          const topIds = new Set(sorted.slice(0, 15).map(n => n.id()));

          cy.batch(() => {
            nodes.forEach(n => {
              const clean = n.data('cleanLabel') || '';
              if (currentZoom > 1.3 || topIds.has(n.id())) {
                n.data('displayLabel', clean);
              } else {
                n.data('displayLabel', '');
              }
            });
          });
        };

        cy.on('zoom', () => {
          updateLabelVisibility();
        });

        // Hover effect: highlight connected edges in bright cyan at 0.8 opacity
        cy.on('mouseover', 'node', function(e) {
          const node = e.target;
          node.addClass('highlighted');
          node.connectedEdges().addClass('highlighted');
        });

        cy.on('mouseout', 'node', function(e) {
          const node = e.target;
          node.removeClass('highlighted');
          node.connectedEdges().removeClass('highlighted');
        });

        const loadGraphData = (data) => {
          cy.elements().remove();
          const elements = [];
          const platformFiltered = selectedPlatform !== 'X';
          const keptIds = new Set();
          const visibleNodes = data.nodes.filter(n =>
            (!platformFiltered || handleMatchesPlatform(n.handle || n.id, selectedPlatform)) &&
            nodeMatchesNiche(n.niche, selectedNiche)
          );
          visibleNodes.forEach(n => keptIds.add(n.id));

          visibleNodes.forEach(n => {
            const rawHandle = n.handle || n.id || '';
            const cleanLabel = rawHandle.replace(/\\.bsky\\.social$/i, '').replace(/^[igth:]+/, '@');
            elements.push({
              data: {
                id: n.id,
                label: n.label || ('@' + rawHandle),
                cleanLabel: cleanLabel,
                displayLabel: '',
                handle: rawHandle,
                node_id: n.node_id,
                pageRank: n.pageRank,
                followersCount: n.followersCount,
                inDegree: n.in_degree,
                outDegree: n.out_degree,
                community: n.community,
                type: n.is_bot ? 'bot' : (n.type || 'follower'),
                isBot: Boolean(n.is_bot || n.isBot || n.type === 'bot'),
                size: n.size || (n.type === 'influencer' ? 44 : 24)
              }
            });
          });

          const visibleEdges = data.edges.filter(e => keptIds.has(e.source) && keptIds.has(e.target));
          visibleEdges.forEach((e, idx) => {
            elements.push({
              data: {
                id: e.id || ('e_' + idx),
                source: e.source,
                target: e.target,
                type: e.type || 'default'
              }
            });
          });
          cy.add(elements);
          cy.layout({ name: 'cose', animate: false }).run();
          updateLabelVisibility();
        };

        if (isTwitter) {
          loadGraphData(TWITTER_GRAPH);
        } else {
          apiGet('/api/graph')
            .then(data => {
              if (data && data.nodes && data.nodes.length > 0) {
                loadGraphData(data);
              } else {
                loadGraphData(BLUESKY_FALLBACK_GRAPH);
                setShowFallbackBanner(true);
              }
            })
            .catch(() => {
              loadGraphData(BLUESKY_FALLBACK_GRAPH);
              setShowFallbackBanner(true);
            });
        }

        // Interactive Tap on Any Node: Promotes it to Ambassador & re-evaluates
        cy.on('tap', 'node', function(evt) {
          const node = evt.target;
          const nodeId = node.id();
          const nodeLabel = (node.data('cleanLabel') || node.data('label') || nodeId).replace(/^@/, '');
          const isBot = node.data('type') === 'bot' || node.data('isBot') === true;

          const existing = candidates.find(c => c.id === nodeId || c.handle.includes(nodeLabel) || c.id.includes(nodeLabel));
          if (existing) {
            handleSelectCandidate(existing);
          } else {
            const backendNodeId = node.data('node_id');
            const backendPR = node.data('pageRank');
            const backendFollowers = node.data('followersCount');
            const hasBackendData = backendNodeId !== undefined && backendNodeId !== null &&
                                   backendPR !== undefined && backendPR !== null;

            const degree = node.connectedEdges().length;
            const seed = charCodeSum(nodeId);
            const calculatedPR = hasBackendData
              ? Number(backendPR).toFixed(4)
              : ((degree * 0.0035) + ((seed % 40) * 0.001) + 0.045).toFixed(4);
            const inDeg = node.data('inDegree');
            const outDeg = node.data('outDegree');
            const totalDeg = (inDeg !== undefined && outDeg !== undefined)
              ? (inDeg + outDeg) : degree;
            const depth = hasBackendData
              ? Math.min(6, Math.max(2, Math.floor(totalDeg / 2) + 2))
              : Math.min(Math.max(2, Math.floor(degree / 3) + 2), 6);
            const reachCount = hasBackendData && backendFollowers > 0
              ? Number(backendFollowers).toLocaleString()
              : Math.floor(degree * 280000 + (seed % 500000) + 150000).toLocaleString();
            const fit = Math.min(99, Math.max(68, Math.floor(totalDeg * 2.0 + (hasBackendData ? 72 : 65))));
            const botRiskPct = isBot ? '94.2% (BOT RING DETECTED)' : ((seed % 35) / 10 + 0.8).toFixed(1) + '% (Safe)';

            const dynamicCandidate = {
              id: nodeId,
              node_id: hasBackendData ? backendNodeId : undefined,
              handle: `${nodeLabel}`,
              platform: isTwitter ? 'Twitter / X (CSV Graph)' : 'Bluesky (AT Protocol)',
              community: hasBackendData && node.data('community') !== undefined
                ? `LPA Cluster ${node.data('community')}`
                : selectedNiche,
              pageRank: calculatedPR,
              cascadeReach: `${reachCount}+ Reach`,
              cascadeDepth: `${depth} Tiers`,
              botRisk: botRiskPct,
              isBotRing: isBot,
              recommended: !isBot && fit >= 85,
              fitScore: `${fit} / 100`
            };

            setCandidates(prev => [dynamicCandidate, ...prev.filter(c => c.id !== nodeId)]);
            setSelectedCandidate(dynamicCandidate);

            // Highlight newly selected node in Cytoscape
            cy.elements().removeStyle();
            node.animate({
              style: {
                'background-color': '#06b6d4',
                'border-color': '#f4f4f5',
                'border-width': 2.5,
                'width': 46,
                'height': 46
              },
              duration: 200
            });
            cy.center(node);
          }
        });

        cy.layout({ name: 'cose', animate: false }).run();
        cyRef.current = cy;
      }, [selectedPlatform, selectedNiche, backendStatus, graphVersion]);
'''

content = content[:cy_start] + new_cy + content[cy_end:]
print('Cytoscape graph logic updated successfully.')

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(content)
print('Saved updated Cytoscape graph to index.html.')
