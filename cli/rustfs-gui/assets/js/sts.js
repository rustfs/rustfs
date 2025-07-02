/**
 * Copyright 2024 RustFS Team
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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