import matplotlib.pyplot as plt
import numpy as np
from typing import Dict, List, Any
import os
from datetime import datetime

class PerformanceChartGenerator:
    """
    Generates professional charts from statistical performance data.
    Creates publication-ready visualizations for academic demonstrations.
    """

    def __init__(self):
        # Set up matplotlib for better-looking charts
        plt.style.use('default')
        plt.rcParams['figure.figsize'] = (12, 8)
        plt.rcParams['font.size'] = 12
        plt.rcParams['axes.grid'] = True
        plt.rcParams['grid.alpha'] = 0.3

    def generate_comparison_chart(self, results: Dict[str, Any], chart_type: str = 'line') -> str:
        """Generate comparison chart from statistical results"""

        opt_stats = results['optimized_stats']
        unopt_stats = results['unoptimized_stats']
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        if chart_type == 'line':
            filename = self._create_line_chart(opt_stats, unopt_stats, timestamp)
        elif chart_type == 'box':
            filename = self._create_box_plot(opt_stats, unopt_stats, timestamp)
        elif chart_type == 'bar':
            filename = self._create_bar_chart(opt_stats, unopt_stats, timestamp)
        else:
            filename = self._create_comprehensive_dashboard(opt_stats, unopt_stats, timestamp)

        return filename

    def _create_line_chart(self, opt_stats, unopt_stats, timestamp: str) -> str:
        """Create line chart showing performance over runs"""

        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))

        # Optimized performance line
        if opt_stats.execution_times:
            runs = range(1, len(opt_stats.execution_times) + 1)
            ax1.plot(runs, opt_stats.execution_times, 'g-o', linewidth=2, markersize=4, label='Optimized')
            ax1.axhline(y=opt_stats.mean_time, color='g', linestyle='--', alpha=0.7, label=f'Mean: {opt_stats.mean_time:.2f}ms')
            ax1.fill_between(runs,
                           [opt_stats.confidence_interval[0]] * len(runs),
                           [opt_stats.confidence_interval[1]] * len(runs),
                           alpha=0.2, color='green', label='95% Confidence')

        ax1.set_title('🚀 Optimized Query Performance Over Time', fontsize=14, fontweight='bold')
        ax1.set_xlabel('Run Number')
        ax1.set_ylabel('Execution Time (ms)')
        ax1.legend()
        ax1.grid(True, alpha=0.3)

        # Unoptimized performance line
        if unopt_stats.execution_times:
            runs = range(1, len(unopt_stats.execution_times) + 1)
            ax2.plot(runs, unopt_stats.execution_times, 'r-o', linewidth=2, markersize=4, label='Unoptimized')
            ax2.axhline(y=unopt_stats.mean_time, color='r', linestyle='--', alpha=0.7, label=f'Mean: {unopt_stats.mean_time:.2f}ms')
            ax2.fill_between(runs,
                           [unopt_stats.confidence_interval[0]] * len(runs),
                           [unopt_stats.confidence_interval[1]] * len(runs),
                           alpha=0.2, color='red', label='95% Confidence')

        ax2.set_title('🐌 Unoptimized Query Performance Over Time', fontsize=14, fontweight='bold')
        ax2.set_xlabel('Run Number')
        ax2.set_ylabel('Execution Time (ms)')
        ax2.legend()
        ax2.grid(True, alpha=0.3)

        plt.tight_layout()

        # Create results directory if it doesn't exist
        results_dir = "results"
        os.makedirs(results_dir, exist_ok=True)

        filename = f"performance_line_chart_{timestamp}.png"
        filepath = os.path.join(results_dir, filename)
        plt.savefig(filepath, dpi=300, bbox_inches='tight')
        plt.close()

        return filename

    def _create_box_plot(self, opt_stats, unopt_stats, timestamp: str) -> str:
        """Create box plot comparing distributions"""

        fig, ax = plt.subplots(figsize=(10, 8))

        data = []
        labels = []
        colors = []

        if opt_stats.execution_times:
            data.append(opt_stats.execution_times)
            labels.append('Optimized\n🚀')
            colors.append('lightgreen')

        if unopt_stats.execution_times:
            data.append(unopt_stats.execution_times)
            labels.append('Unoptimized\n🐌')
            colors.append('lightcoral')

        if data:
            bp = ax.boxplot(data, labels=labels, patch_artist=True, notch=True)

            # Color the boxes
            for patch, color in zip(bp['boxes'], colors):
                patch.set_facecolor(color)
                patch.set_alpha(0.7)

        ax.set_title('📊 Performance Distribution Comparison', fontsize=16, fontweight='bold')
        ax.set_ylabel('Execution Time (ms)')
        ax.grid(True, alpha=0.3)

        # Add statistics text
        if opt_stats.execution_times and unopt_stats.execution_times:
            speedup = unopt_stats.mean_time / opt_stats.mean_time
            improvement = ((unopt_stats.mean_time - opt_stats.mean_time) / unopt_stats.mean_time) * 100

            stats_text = f"""
📈 Performance Summary:
• Speedup: {speedup:.1f}x faster
• Improvement: {improvement:.1f}%
• Optimized Mean: {opt_stats.mean_time:.2f}ms
• Unoptimized Mean: {unopt_stats.mean_time:.2f}ms
            """

            ax.text(0.02, 0.98, stats_text, transform=ax.transAxes, fontsize=10,
                   verticalalignment='top', bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8))

        plt.tight_layout()

        # Create results directory if it doesn't exist
        results_dir = "results"
        os.makedirs(results_dir, exist_ok=True)

        filename = f"performance_box_plot_{timestamp}.png"
        filepath = os.path.join(results_dir, filename)
        plt.savefig(filepath, dpi=300, bbox_inches='tight')
        plt.close()

        return filename

    def _create_bar_chart(self, opt_stats, unopt_stats, timestamp: str) -> str:
        """Create bar chart of summary statistics"""

        fig, ax = plt.subplots(figsize=(12, 8))

        categories = ['Mean Time', 'Min Time', 'Max Time', 'Std Deviation']
        optimized_values = [opt_stats.mean_time, opt_stats.min_time, opt_stats.max_time, opt_stats.std_deviation]
        unoptimized_values = [unopt_stats.mean_time, unopt_stats.min_time, unopt_stats.max_time, unopt_stats.std_deviation]

        x = np.arange(len(categories))
        width = 0.35

        bars1 = ax.bar(x - width/2, optimized_values, width, label='Optimized 🚀', color='lightgreen', alpha=0.8)
        bars2 = ax.bar(x + width/2, unoptimized_values, width, label='Unoptimized 🐌', color='lightcoral', alpha=0.8)

        ax.set_title('📊 Performance Statistics Comparison', fontsize=16, fontweight='bold')
        ax.set_ylabel('Time (ms)')
        ax.set_xlabel('Metric')
        ax.set_xticks(x)
        ax.set_xticklabels(categories)
        ax.legend()
        ax.grid(True, alpha=0.3)

        # Add value labels on bars
        def add_value_labels(bars):
            for bar in bars:
                height = bar.get_height()
                ax.annotate(f'{height:.2f}',
                           xy=(bar.get_x() + bar.get_width() / 2, height),
                           xytext=(0, 3),  # 3 points vertical offset
                           textcoords="offset points",
                           ha='center', va='bottom', fontsize=10)

        add_value_labels(bars1)
        add_value_labels(bars2)

        plt.tight_layout()

        # Create results directory if it doesn't exist
        results_dir = "results"
        os.makedirs(results_dir, exist_ok=True)

        filename = f"performance_bar_chart_{timestamp}.png"
        filepath = os.path.join(results_dir, filename)
        plt.savefig(filepath, dpi=300, bbox_inches='tight')
        plt.close()

        return filename

    def _create_comprehensive_dashboard(self, opt_stats, unopt_stats, timestamp: str) -> str:
        """Create comprehensive dashboard with multiple visualizations"""

        fig = plt.figure(figsize=(16, 12))

        # Create subplots
        gs = fig.add_gridspec(3, 2, height_ratios=[1, 1, 1], width_ratios=[1, 1])

        # 1. Line plot of performance over time
        ax1 = fig.add_subplot(gs[0, :])
        if opt_stats.execution_times and unopt_stats.execution_times:
            runs = range(1, max(len(opt_stats.execution_times), len(unopt_stats.execution_times)) + 1)

            if opt_stats.execution_times:
                ax1.plot(range(1, len(opt_stats.execution_times) + 1), opt_stats.execution_times,
                        'g-o', linewidth=2, markersize=4, label='Optimized 🚀')

            if unopt_stats.execution_times:
                ax1.plot(range(1, len(unopt_stats.execution_times) + 1), unopt_stats.execution_times,
                        'r-o', linewidth=2, markersize=4, label='Unoptimized 🐌')

        ax1.set_title('Performance Over Time', fontweight='bold')
        ax1.set_xlabel('Run Number')
        ax1.set_ylabel('Execution Time (ms)')
        ax1.legend()
        ax1.grid(True, alpha=0.3)

        # 2. Box plot comparison
        ax2 = fig.add_subplot(gs[1, 0])
        data = []
        labels = []
        if opt_stats.execution_times:
            data.append(opt_stats.execution_times)
            labels.append('Optimized')
        if unopt_stats.execution_times:
            data.append(unopt_stats.execution_times)
            labels.append('Unoptimized')

        if data:
            bp = ax2.boxplot(data, labels=labels, patch_artist=True)
            colors = ['lightgreen', 'lightcoral']
            for patch, color in zip(bp['boxes'], colors[:len(bp['boxes'])]):
                patch.set_facecolor(color)
                patch.set_alpha(0.7)

        ax2.set_title('Distribution Comparison', fontweight='bold')
        ax2.set_ylabel('Execution Time (ms)')
        ax2.grid(True, alpha=0.3)

        # 3. Bar chart of statistics
        ax3 = fig.add_subplot(gs[1, 1])
        categories = ['Mean', 'Min', 'Max', 'Std Dev']
        opt_values = [opt_stats.mean_time, opt_stats.min_time, opt_stats.max_time, opt_stats.std_deviation]
        unopt_values = [unopt_stats.mean_time, unopt_stats.min_time, unopt_stats.max_time, unopt_stats.std_deviation]

        x = np.arange(len(categories))
        width = 0.35
        ax3.bar(x - width/2, opt_values, width, label='Optimized', color='lightgreen', alpha=0.8)
        ax3.bar(x + width/2, unopt_values, width, label='Unoptimized', color='lightcoral', alpha=0.8)

        ax3.set_title('Statistics Summary', fontweight='bold')
        ax3.set_ylabel('Time (ms)')
        ax3.set_xticks(x)
        ax3.set_xticklabels(categories, rotation=45)
        ax3.legend()
        ax3.grid(True, alpha=0.3)

        # 4. Summary statistics text
        ax4 = fig.add_subplot(gs[2, :])
        ax4.axis('off')

        if opt_stats.execution_times and unopt_stats.execution_times:
            speedup = unopt_stats.mean_time / opt_stats.mean_time
            improvement = ((unopt_stats.mean_time - opt_stats.mean_time) / unopt_stats.mean_time) * 100
            time_saved = unopt_stats.mean_time - opt_stats.mean_time

            summary_text = f"""
🎯 PERFORMANCE ANALYSIS SUMMARY

📊 Statistical Results ({opt_stats.run_count} runs):
• Mean Speedup: {speedup:.1f}x faster
• Performance Improvement: {improvement:.1f}%
• Average Time Saved: {time_saved:.2f}ms per query

🚀 Optimized Approach:
• Mean: {opt_stats.mean_time:.2f}ms ± {opt_stats.std_deviation:.2f}ms
• Range: {opt_stats.min_time:.2f}ms - {opt_stats.max_time:.2f}ms
• Success Rate: {opt_stats.success_rate:.1f}%

🐌 Unoptimized Approach:
• Mean: {unopt_stats.mean_time:.2f}ms ± {unopt_stats.std_deviation:.2f}ms
• Range: {unopt_stats.min_time:.2f}ms - {unopt_stats.max_time:.2f}ms
• Success Rate: {unopt_stats.success_rate:.1f}%

💡 Recommendation: {'High Priority' if speedup > 10 else 'Recommended' if speedup > 2 else 'Optional'} optimization for production use
            """

            ax4.text(0.05, 0.95, summary_text, transform=ax4.transAxes, fontsize=11,
                    verticalalignment='top', fontfamily='monospace',
                    bbox=dict(boxstyle='round,pad=1', facecolor='lightblue', alpha=0.8))

        plt.suptitle('🔬 Kedai Kopi Performance Analysis Dashboard', fontsize=18, fontweight='bold')
        plt.tight_layout()

        # Create results directory if it doesn't exist
        results_dir = "results"
        os.makedirs(results_dir, exist_ok=True)

        filename = f"performance_dashboard_{timestamp}.png"
        filepath = os.path.join(results_dir, filename)
        plt.savefig(filepath, dpi=300, bbox_inches='tight')
        plt.close()

        return filename
