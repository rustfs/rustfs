window.switchTab = function (tabId) {
    // Hide everything
    document.querySelectorAll('.tab-content').forEach(content => {
        content.classList.add('hidden');
    });

    // Reset all label styles
    document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.classList.remove('border-b-2', 'border-black');
        btn.classList.add('text-gray-500');
    });

    // Displays the selected content
    const activeContent = document.getElementById(tabId);
    if (activeContent) {
        activeContent.classList.remove('hidden');
    }

    // Updates the selected label style
    const activeBtn = document.querySelector(`[data-tab="${tabId}"]`);
    if (activeBtn) {
        activeBtn.classList.add('border-b-2', 'border-black');
        activeBtn.classList.remove('text-gray-500');
    }
};

window.togglePassword = function (button) {
    const input = button.parentElement.querySelector('input[type="password"], input[type="text"]');
    if (input) {
        input.type = input.type === 'password' ? 'text' : 'password';
    }
};