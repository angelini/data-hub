function resetSelects(fieldset, published) {
    let hubs = fieldset.querySelector('.hub-dropdown');
    let datasets = fieldset.querySelector('.dataset-dropdown');
    let versions = fieldset.querySelector('.version-dropdown');

    let hubKey = hubs.value + ':' + hubs.options[hubs.selectedIndex].innerText;

    if (datasets.dataset.parent != hubs.value) {
        datasets.innerHTML = '';

        Object.keys(published[hubKey]).forEach(key => {
            let [id, name] = key.split(':');
            datasets.appendChild(new Option(name, id));
        });

        datasets.dataset.parent = hubs.value;
    }

    let datasetKey = datasets.value + ':' + datasets.options[datasets.selectedIndex].innerText;

    if (versions.dataset.parent != datasets.value) {
        versions.innerHTML = '';

        published[hubKey][datasetKey].forEach(version => {
            versions.appendChild(new Option(version, version));
        });

        versions.dataset.parent = datasets.value;
    }
}

document.addEventListener('DOMContentLoaded', () => {
    let fieldset = document.getElementById('version-dropdown');
    fieldset.addEventListener('change', event => {
        resetSelects(fieldset, publishedVersions);
    });
    resetSelects(fieldset, publishedVersions);
});
