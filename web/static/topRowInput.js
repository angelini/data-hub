function hideAndClearRow(event) {
    event.preventDefault();
    let row = event.target.closest('tr');
    row.querySelectorAll('input').forEach(input => {
        input.value = '';
    });
    row.hidden = true;
}

function showInputRow(root) {
    root.querySelector('.input-row').hidden = false;
}

document.addEventListener('DOMContentLoaded', () => {
    document.querySelectorAll('.top-row-input .add-row').forEach(button => {
        button.addEventListener('click', event => {
            event.preventDefault();
            showInputRow(event.target.closest('form'));
        });
    });

    document.querySelector('.top-row-input .remove-row').addEventListener('click', hideAndClearRow);

    document.querySelector('.top-row-input .submit-row').addEventListener('click', event => {
        event.target.closest('form').submit();
    });
});
