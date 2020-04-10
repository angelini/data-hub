function label(node) {
    if (node.is_same_hub) {
        return `${node.dataset_name} ${node.version}`
    } else {
        return `${node.hub_name} ${node.dataset_name} - ${node.version}`
    }
}

function drawDependencies(container, dependencies) {
    let g = new dagreD3.graphlib.Graph();

    g.setGraph({});
    g.setDefaultEdgeLabel(function() { return {}; });

    let nodes = new Set();
    dependencies.forEach(dep => {
        if (!nodes.has(dep.parent.key)) {
            let style = dep.parent.is_selected ? 'fill: #afa' : '';
            g.setNode(dep.parent.key, {label: label(dep.parent), style: style});
            nodes.add(dep.parent.key);
        }

        if (!nodes.has(dep.child.key)) {
            let style = dep.child.is_selected ? 'fill: #afa' : '';
            g.setNode(dep.child.key, {label: label(dep.child), style: style});
            nodes.add(dep.child.key);
        }
    });

    dependencies.forEach(dep => {
        g.setEdge(dep.parent.key, dep.child.key);
    });

    let render = new dagreD3.render();
    render(container, g);
    return g
}

function centerGraph(g, svg, zoom) {
    let margin = 40;
    let svgWidth = svg.node().clientWidth;
    let graphSize = {width: g.graph().width, height: g.graph().height};

    let scale = Math.min(1, (svgWidth - margin) / graphSize.width);

    svg.call(zoom.transform, d3.zoomIdentity.translate(
        (svgWidth - graphSize.width * scale) / 2, 20
    ).scale(scale));

    if (!svg.attr('height')) {
        svg.attr('height', graphSize.height * scale + margin);
    }
}

document.addEventListener('DOMContentLoaded', () => {
    let container = document.getElementById('dependencies');
    let svg = d3.select('#dependencies svg');
    let inner = svg.select('g');

    let zoom = d3.zoom().on('zoom', () => {
        inner.attr('transform', d3.event.transform);
    });
    if (container.classList.contains('full')) {
        svg.call(zoom);
    }

    let g = drawDependencies(inner, versionDependencies);

    centerGraph(g, svg, zoom);
});
