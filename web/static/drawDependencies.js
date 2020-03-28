function label(node) {
    if (node.is_same_hub) {
        return `${node.dataset_name} - ${node.version}`
    } else {
        return `${node.hub_name} - ${node.dataset_name} - ${node.version}`
    }
}

function drawDependencies(container, dependencies, version) {
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
}

document.addEventListener('DOMContentLoaded', () => {
    let svg = d3.select('#dependencies svg');
    let inner = svg.select('g');
    let zoom = d3.zoom().on('zoom', () => {
        inner.attr('transform', d3.event.transform);
    });
    svg.call(zoom);
    drawDependencies(inner, versionDependencies, selectedVersion);
});
