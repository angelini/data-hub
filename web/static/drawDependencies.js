function parentKey(dep) {
    return `${dep.parent_hub_id}:${dep.parent_dataset_id}:${dep.parent_version}`;
}

function childKey(dep) {
    return `${dep.child_hub_id}:${dep.child_dataset_id}:${dep.child_version}`;
}

function drawDependencies(container, dependencies) {
    let g = new dagreD3.graphlib.Graph();

    g.setGraph({});
    g.setDefaultEdgeLabel(function() { return {}; });

    let nodes = new Set();
    dependencies.forEach(dep => {
        let parent = parentKey(dep);
        if (!nodes.has(parent)) {
            g.setNode(parent, {label: `${dep.parent_hub_name}:${dep.parent_dataset_name}:${dep.parent_version}`});
            nodes.add(parent);
        }
        let child = childKey(dep);
        if (!nodes.has(child)) {
            g.setNode(child, {label: `${dep.child_hub_name}:${dep.child_dataset_name}:${dep.child_version}`});
            nodes.add(child);
        }
    });

    dependencies.forEach(dep => {
        g.setEdge(`${dep.parent_hub_id}:${dep.parent_dataset_id}:${dep.parent_version}`,
                  `${dep.child_hub_id}:${dep.child_dataset_id}:${dep.child_version}`);
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
    drawDependencies(inner, versionDependencies);
});
