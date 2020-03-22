function removeRow(event) {
    event.preventDefault();
    if (event.target.closest('.dynamic-rows').querySelectorAll('tbody tr').length > 1) {
        event.target.closest('tr').remove();
    }
}

function updateHiddenText(event) {
    let hidden = event.target.nextElementSibling;
    hidden.value = event.target.checked.toString();
}

function addRow(tableBody, rows) {
    let last = rows[rows.length - 1];
    let newRow = last.cloneNode(true);

    newRow.querySelectorAll('input[type=text]:not([hidden])').forEach(input => {
        input.value = '';
    });
    newRow.querySelectorAll('input[type=text][hidden]').forEach(input => {
        input.value = 'false';
    });
    newRow.querySelectorAll('input[type=checkbox]').forEach(input => {
        input.checked = false;
        input.addEventListener('change', updateHiddenText);
    });

    newRow.querySelector('.remove-row').addEventListener('click', removeRow);

    tableBody.appendChild(newRow);
}

document.addEventListener('DOMContentLoaded', () => {
    document.querySelectorAll('.dynamic-rows .add-row').forEach(button => {
            button.addEventListener('click', event => {
                event.preventDefault();
                let tableBody = event.target.closest('form').querySelector('tbody');
                addRow(tableBody, tableBody.querySelectorAll('tr'));
            });
    });

    document.querySelectorAll('.dynamic-rows .remove-row').forEach(button => {
        button.addEventListener('click', removeRow);
    })

    document.querySelectorAll('.dynamic-rows input[type=checkbox]').forEach(button => {
        button.addEventListener('change', updateHiddenText);
    })
});
