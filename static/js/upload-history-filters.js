/**
 * Upload History Filters Module
 * Handles real-time search, dropdown filters, and autocomplete functionality
 */

class UploadHistoryFilters {
    constructor() {
        this.allData = [];
        this.filteredData = [];
        this.activeFilters = {
            batchId: '',
            location: '',
            pandaName: ''
        };
        
        // Pagination state
        this.currentPage = 1;
        this.perPage = 10;
        this.totalRecords = 0;
        this.totalPages = 0;
        this.paginationInfo = {};
        
        // Autocomplete state
        this.autocompleteTimeout = null;
        this.currentHighlightIndex = -1;
        this.autocompleteVisible = false;
        
        this.init();
    }

    init() {
        this.setupEventListeners();
        this.loadInitialData();
        this.populateDropdowns();
    }

    setupEventListeners() {
        // Batch ID search with autocomplete
        const batchSearchInput = document.getElementById('batch-search');
        if (batchSearchInput) {
            batchSearchInput.addEventListener('input', (e) => this.handleBatchSearch(e));
            batchSearchInput.addEventListener('keydown', (e) => this.handleKeyNavigation(e));
            batchSearchInput.addEventListener('focus', () => this.handleSearchFocus());
            batchSearchInput.addEventListener('blur', () => this.handleSearchBlur());
        }

        // Dropdown filters
        const locationFilter = document.getElementById('location-filter');
        if (locationFilter) {
            locationFilter.addEventListener('change', (e) => this.handleLocationFilter(e));
        }

        const pandaFilter = document.getElementById('panda-filter');
        if (pandaFilter) {
            pandaFilter.addEventListener('change', (e) => this.handlePandaFilter(e));
        }

        // Action buttons
        const clearBtn = document.getElementById('clear-filters');
        if (clearBtn) {
            clearBtn.addEventListener('click', () => this.clearAllFilters());
        }

        // Hide autocomplete when clicking outside
        document.addEventListener('click', (e) => {
            if (!e.target.closest('.search-container')) {
                this.hideAutocomplete();
            }
        });
    }

    async loadInitialData() {
        try {
            await this.loadPageData(1);
        } catch (error) {
            console.error('Error loading upload history:', error);
            this.showError('Failed to load upload history data');
        }
    }

    async loadPageData(page = 1) {
        try {
            const response = await fetch(`/get_upload_history?page=${page}&per_page=${this.perPage}`);
            const data = await response.json();
            
            if (data.history) {
                this.allData = data.history;
                this.filteredData = [...this.allData];
                this.currentPage = page;
                this.paginationInfo = data.pagination;
                this.totalRecords = data.pagination.total_records;
                this.totalPages = data.pagination.total_pages;
                
                this.renderTable();
                this.renderPagination();
                this.updateFilterStatus();
            }
        } catch (error) {
            console.error('Error loading page data:', error);
            this.showError('Failed to load upload history data');
        }
    }

    async populateDropdowns() {
        try {
            const response = await fetch('/get_upload_history_filter_options');
            const data = await response.json();
            
            if (data.success) {
                this.populateLocationDropdown(data.locations);
                this.populatePandaDropdown(data.panda_names);
            }
        } catch (error) {
            console.error('Error loading filter options:', error);
        }
    }

    populateLocationDropdown(locations) {
        const select = document.getElementById('location-filter');
        if (!select) return;

        select.innerHTML = '<option value="">All Locations</option>';
        locations.forEach(location => {
            if (location) {
                select.innerHTML += `<option value="${location}">${location}</option>`;
            }
        });
    }

    populatePandaDropdown(pandaNames) {
        const select = document.getElementById('panda-filter');
        if (!select) return;

        select.innerHTML = '<option value="">All Panda Names</option>';
        pandaNames.forEach(name => {
            if (name) {
                select.innerHTML += `<option value="${name}">${name}</option>`;
            }
        });
    }

    handleBatchSearch(event) {
        const query = event.target.value.trim();
        this.activeFilters.batchId = query;
        
        // Debounce the search
        clearTimeout(this.autocompleteTimeout);
        this.autocompleteTimeout = setTimeout(() => {
            this.performSearch();
            if (query.length >= 1) {
                this.showAutocomplete(query);
            } else {
                this.hideAutocomplete();
            }
        }, 300);
    }

    handleLocationFilter(event) {
        this.activeFilters.location = event.target.value;
        this.performSearch();
    }

    handlePandaFilter(event) {
        this.activeFilters.pandaName = event.target.value;
        this.performSearch();
    }

    performSearch() {
        // For now, we'll filter on the current page data
        // In a full implementation, you might want to send filters to the server
        this.filteredData = this.allData.filter(item => {
            const batchMatch = !this.activeFilters.batchId || 
                item.batch_id.toLowerCase().includes(this.activeFilters.batchId.toLowerCase());
            
            const locationMatch = !this.activeFilters.location || 
                item.location === this.activeFilters.location;
                
            const pandaMatch = !this.activeFilters.pandaName || 
                item.pandas_name === this.activeFilters.pandaName;
                
            return batchMatch && locationMatch && pandaMatch;
        });
        
        this.renderTable();
        this.updateFilterStatus();
    }

    showAutocomplete(query) {
        const matchingBatchIds = this.allData
            .filter(item => item.batch_id.toLowerCase().includes(query.toLowerCase()))
            .map(item => item.batch_id)
            .filter((value, index, self) => self.indexOf(value) === index) // Remove duplicates
            .slice(0, 10); // Limit to 10 results

        this.renderAutocomplete(matchingBatchIds, query);
    }

    renderAutocomplete(batchIds, query) {
        const dropdown = document.getElementById('autocomplete-dropdown');
        if (!dropdown) return;

        if (batchIds.length === 0) {
            dropdown.innerHTML = '<div class="autocomplete-no-results">No matching batch IDs found</div>';
        } else {
            let html = '';
            batchIds.forEach((batchId, index) => {
                const regex = new RegExp(`(${query})`, 'gi');
                const highlightedText = batchId.replace(regex, '<strong>$1</strong>');
                html += `<div class="autocomplete-item" data-batch-id="${batchId}" data-index="${index}">${highlightedText}</div>`;
            });
            dropdown.innerHTML = html;

            // Add click handlers
            dropdown.querySelectorAll('.autocomplete-item').forEach(item => {
                item.addEventListener('click', () => {
                    this.selectAutocompleteItem(item.dataset.batchId);
                });
            });
        }

        dropdown.style.display = 'block';
        this.autocompleteVisible = true;
        this.currentHighlightIndex = -1;
    }

    selectAutocompleteItem(batchId) {
        const input = document.getElementById('batch-search');
        if (input) {
            input.value = batchId;
            this.activeFilters.batchId = batchId;
            this.performSearch();
        }
        this.hideAutocomplete();
    }

    hideAutocomplete() {
        const dropdown = document.getElementById('autocomplete-dropdown');
        if (dropdown) {
            dropdown.style.display = 'none';
        }
        this.autocompleteVisible = false;
        this.currentHighlightIndex = -1;
    }

    handleKeyNavigation(event) {
        if (!this.autocompleteVisible) return;

        const items = document.querySelectorAll('.autocomplete-item');
        
        switch (event.keyCode) {
            case 38: // Up arrow
                event.preventDefault();
                this.navigateAutocomplete('up', items);
                break;
            case 40: // Down arrow
                event.preventDefault();
                this.navigateAutocomplete('down', items);
                break;
            case 13: // Enter
                event.preventDefault();
                if (this.currentHighlightIndex >= 0 && items[this.currentHighlightIndex]) {
                    this.selectAutocompleteItem(items[this.currentHighlightIndex].dataset.batchId);
                } else {
                    this.hideAutocomplete();
                    this.performSearch();
                }
                break;
            case 27: // Escape
                event.preventDefault();
                this.hideAutocomplete();
                break;
        }
    }

    navigateAutocomplete(direction, items) {
        // Remove current highlight
        items.forEach(item => item.classList.remove('highlighted'));

        if (direction === 'down') {
            this.currentHighlightIndex = this.currentHighlightIndex < items.length - 1 
                ? this.currentHighlightIndex + 1 : 0;
        } else {
            this.currentHighlightIndex = this.currentHighlightIndex > 0 
                ? this.currentHighlightIndex - 1 : items.length - 1;
        }

        if (items[this.currentHighlightIndex]) {
            items[this.currentHighlightIndex].classList.add('highlighted');
            items[this.currentHighlightIndex].scrollIntoView({ block: 'nearest' });
        }
    }

    handleSearchFocus() {
        const input = document.getElementById('batch-search');
        const query = input ? input.value.trim() : '';
        if (query.length >= 1) {
            this.showAutocomplete(query);
        }
    }

    handleSearchBlur() {
        setTimeout(() => {
            this.hideAutocomplete();
        }, 150);
    }

    clearAllFilters() {
        // Clear form inputs
        const batchSearch = document.getElementById('batch-search');
        const locationFilter = document.getElementById('location-filter');
        const pandaFilter = document.getElementById('panda-filter');

        if (batchSearch) batchSearch.value = '';
        if (locationFilter) locationFilter.value = '';
        if (pandaFilter) pandaFilter.value = '';

        // Clear active filters
        this.activeFilters = {
            batchId: '',
            location: '',
            pandaName: ''
        };

        // Reset data and reload first page
        this.loadPageData(1);
        this.hideAutocomplete();
    }

    renderTable() {
        const tableBody = document.getElementById("history-table-body");
        if (!tableBody) return;

        if (this.filteredData.length === 0) {
            const noResultsKey = this.hasActiveFilters() ? 'no_results_with_filters' : 'no_records_found';
            tableBody.innerHTML = `<tr><td colspan="10" class="text-center">${window.languageManager ? window.languageManager.getTranslation(noResultsKey) : 'No records found'}</td></tr>`;
            return;
        }

        tableBody.innerHTML = '';
        this.filteredData.forEach(row => {
            const tr = document.createElement("tr");
            tr.innerHTML = `
                <td>${row.upload_date}</td>
                <td>${row.batch_id}</td>
                <td>${row.location}</td>
                <td>${row.pandas_name}</td>
                <td>${row.bahi_name}</td>
                <td>${row.upload_type}</td>
                <td>${row.image_count}</td>
                <td>${row.approved_count}</td>
                <td>${row.rejected_count}</td>
                <td><button onclick="downloadBatchReport('${row.batch_id}')">${window.languageManager ? window.languageManager.getTranslation('download_qc_report') : 'Download QC Report'}</button></td>
            `;
            tableBody.appendChild(tr);
        });
    }



    renderActiveFilters() {
        const container = document.getElementById('active-filters');
        if (!container) return;

        container.innerHTML = '';
        
        Object.entries(this.activeFilters).forEach(([key, value]) => {
            if (value) {
                const tag = document.createElement('div');
                tag.className = 'filter-tag';
                
                let label = '';
                switch (key) {
                    case 'batchId': label = 'Batch ID'; break;
                    case 'location': label = 'Location'; break;
                    case 'pandaName': label = 'Panda Name'; break;
                }
                
                tag.innerHTML = `
                    ${label}: ${value}
                    <button class="filter-tag-remove" onclick="uploadHistoryFilters.removeFilter('${key}')">&times;</button>
                `;
                container.appendChild(tag);
            }
        });
    }

    removeFilter(filterKey) {
        this.activeFilters[filterKey] = '';
        
        // Update corresponding form element
        switch (filterKey) {
            case 'batchId':
                const batchSearch = document.getElementById('batch-search');
                if (batchSearch) batchSearch.value = '';
                break;
            case 'location':
                const locationFilter = document.getElementById('location-filter');
                if (locationFilter) locationFilter.value = '';
                break;
            case 'pandaName':
                const pandaFilter = document.getElementById('panda-filter');
                if (pandaFilter) pandaFilter.value = '';
                break;
        }
        
        this.performSearch();
    }

    hasActiveFilters() {
        return Object.values(this.activeFilters).some(value => value !== '');
    }

    renderPagination() {
        const paginationContainer = document.getElementById('pagination-container');
        const paginationControls = document.getElementById('pagination-controls');
        const paginationInfoText = document.getElementById('pagination-info-text');
        
        if (!paginationContainer || !paginationControls || !this.paginationInfo) return;

        // Show pagination container only if there are records and multiple pages
        paginationContainer.style.display = (this.totalRecords > 0 && this.totalPages > 1) ? 'block' : 'none';
        
        if (this.totalPages <= 1) return;

        // Update pagination info text
        const startRecord = ((this.currentPage - 1) * this.perPage) + 1;
        const endRecord = Math.min(this.currentPage * this.perPage, this.totalRecords);
        const showingText = window.languageManager ? window.languageManager.getTranslation('showing_records') : 'Showing records';
        const ofText = window.languageManager ? window.languageManager.getTranslation('of') : 'of';
        paginationInfoText.textContent = `${showingText} ${startRecord}-${endRecord} ${ofText} ${this.totalRecords}`;

        // Clear existing pagination controls
        paginationControls.innerHTML = '';

        // Previous button
        const prevText = window.languageManager ? window.languageManager.getTranslation('previous') : 'Previous';
        const prevLi = document.createElement('li');
        prevLi.className = `page-item ${!this.paginationInfo.has_prev ? 'disabled' : ''}`;
        prevLi.innerHTML = `<a class="page-link" href="#" data-page="${this.currentPage - 1}">${prevText}</a>`;
        paginationControls.appendChild(prevLi);

        // Page numbers
        const startPage = Math.max(1, this.currentPage - 2);
        const endPage = Math.min(this.totalPages, this.currentPage + 2);

        // First page if not in range
        if (startPage > 1) {
            const firstLi = document.createElement('li');
            firstLi.className = 'page-item';
            firstLi.innerHTML = '<a class="page-link" href="#" data-page="1">1</a>';
            paginationControls.appendChild(firstLi);
            
            if (startPage > 2) {
                const ellipsisLi = document.createElement('li');
                ellipsisLi.className = 'page-item disabled';
                ellipsisLi.innerHTML = '<span class="page-link">...</span>';
                paginationControls.appendChild(ellipsisLi);
            }
        }

        // Page range
        for (let i = startPage; i <= endPage; i++) {
            const pageLi = document.createElement('li');
            pageLi.className = `page-item ${i === this.currentPage ? 'active' : ''}`;
            pageLi.innerHTML = `<a class="page-link" href="#" data-page="${i}">${i}</a>`;
            paginationControls.appendChild(pageLi);
        }

        // Last page if not in range
        if (endPage < this.totalPages) {
            if (endPage < this.totalPages - 1) {
                const ellipsisLi = document.createElement('li');
                ellipsisLi.className = 'page-item disabled';
                ellipsisLi.innerHTML = '<span class="page-link">...</span>';
                paginationControls.appendChild(ellipsisLi);
            }
            
            const lastLi = document.createElement('li');
            lastLi.className = 'page-item';
            lastLi.innerHTML = `<a class="page-link" href="#" data-page="${this.totalPages}">${this.totalPages}</a>`;
            paginationControls.appendChild(lastLi);
        }

        // Next button
        const nextText = window.languageManager ? window.languageManager.getTranslation('next') : 'Next';
        const nextLi = document.createElement('li');
        nextLi.className = `page-item ${!this.paginationInfo.has_next ? 'disabled' : ''}`;
        nextLi.innerHTML = `<a class="page-link" href="#" data-page="${this.currentPage + 1}">${nextText}</a>`;
        paginationControls.appendChild(nextLi);

        // Add click handlers
        paginationControls.addEventListener('click', (e) => {
            e.preventDefault();
            if (e.target.tagName === 'A' && !e.target.closest('.disabled')) {
                const page = parseInt(e.target.dataset.page);
                if (page && page !== this.currentPage) {
                    this.loadPageData(page);
                }
            }
        });
    }

    updateFilterStatus() {
        const statusElement = document.getElementById('filter-status');
        if (!statusElement) return;

        const activeFilterCount = Object.values(this.activeFilters).filter(value => value !== '').length;
        const currentResults = this.filteredData.length;
        const totalResults = this.totalRecords;
        
        let statusText = `Showing ${currentResults} of ${totalResults} records`;
        if (activeFilterCount > 0) {
            statusText += ` (${activeFilterCount} filter${activeFilterCount > 1 ? 's' : ''} applied)`;
        }
        
        statusElement.textContent = statusText;
        this.renderActiveFilters();
    }

    showError(message) {
        const tableBody = document.getElementById("history-table-body");
        if (tableBody) {
            tableBody.innerHTML = `<tr><td colspan="10" class="text-center text-danger">${message}</td></tr>`;
        }
        
        // Hide pagination on error
        const paginationContainer = document.getElementById('pagination-container');
        if (paginationContainer) {
            paginationContainer.style.display = 'none';
        }
    }
}

// Initialize filters when DOM is loaded
document.addEventListener('DOMContentLoaded', function() {
    // Wait a bit to ensure other scripts have loaded
    setTimeout(() => {
        window.uploadHistoryFilters = new UploadHistoryFilters();
    }, 100);
}); 