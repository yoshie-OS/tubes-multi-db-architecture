# src/core/statistical_performance_analyzer.py
import time
import statistics
import json
import csv
from datetime import datetime
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)

@dataclass
class StatisticalResult:
    """Container for statistical analysis results"""
    run_count: int
    execution_times: List[float]
    mean_time: float
    std_deviation: float
    min_time: float
    max_time: float
    success_rate: float
    successful_runs: int
    failed_runs: int
    confidence_interval: tuple

class StatisticalPerformanceAnalyzer:
    """
    Enhanced performance analyzer that runs multiple iterations
    and provides statistical analysis with visualization options.
    """

    def __init__(self, performance_analyzer):
        self.performance_analyzer = performance_analyzer

    def run_statistical_analysis(self, query_config: Dict[str, Any],
                                run_count: int = 10) -> Dict[str, Any]:
        """Run multiple iterations and perform statistical analysis"""

        print(f"\n🔬 {self.colored_text(f'STATISTICAL PERFORMANCE ANALYSIS ({run_count} runs)', 'bold')}")
        print("=" * 60)

        # Storage for results
        optimized_times = []
        unoptimized_times = []
        optimized_failures = 0
        unoptimized_failures = 0

        # Run multiple iterations
        for i in range(run_count):
            print(f"🔄 Run {i+1}/{run_count}...", end=" ", flush=True)

            try:
                # Run single comparison
                comparison = self.performance_analyzer.compare_optimization_scenarios(query_config)

                # Record results
                if comparison.optimized_result.success:
                    optimized_times.append(comparison.optimized_result.execution_time_ms)
                else:
                    optimized_failures += 1

                if comparison.unoptimized_result.success:
                    unoptimized_times.append(comparison.unoptimized_result.execution_time_ms)
                else:
                    unoptimized_failures += 1

                print("✅")

                # Small delay to avoid overwhelming the database
                time.sleep(0.1)

            except Exception as e:
                print(f"❌ Error: {e}")
                optimized_failures += 1
                unoptimized_failures += 1

        # Calculate statistics
        optimized_stats = self._calculate_statistics(optimized_times, run_count)
        unoptimized_stats = self._calculate_statistics(unoptimized_times, run_count)

        # Display results
        self._display_statistical_results(optimized_stats, unoptimized_stats, query_config)

        # Return comprehensive results
        return {
            'query_config': query_config,
            'run_count': run_count,
            'optimized_stats': optimized_stats,
            'unoptimized_stats': unoptimized_stats,
            'timestamp': datetime.now().isoformat(),
            'analysis': self._generate_statistical_analysis(optimized_stats, unoptimized_stats)
        }

    def _calculate_statistics(self, times: List[float], total_runs: int) -> StatisticalResult:
        """Calculate comprehensive statistics for execution times"""

        if not times:
            return StatisticalResult(
                run_count=total_runs,
                execution_times=[],
                mean_time=0.0,
                std_deviation=0.0,
                min_time=0.0,
                max_time=0.0,
                success_rate=0.0,
                successful_runs=0,
                failed_runs=total_runs,
                confidence_interval=(0.0, 0.0)
            )

        successful_runs = len(times)
        failed_runs = total_runs - successful_runs
        success_rate = (successful_runs / total_runs) * 100

        mean_time = statistics.mean(times)
        std_dev = statistics.stdev(times) if len(times) > 1 else 0.0
        min_time = min(times)
        max_time = max(times)

        # Calculate 95% confidence interval (approximate)
        if len(times) > 1:
            margin_error = 1.96 * (std_dev / (len(times) ** 0.5))
            conf_interval = (mean_time - margin_error, mean_time + margin_error)
        else:
            conf_interval = (mean_time, mean_time)

        return StatisticalResult(
            run_count=total_runs,
            execution_times=times,
            mean_time=mean_time,
            std_deviation=std_dev,
            min_time=min_time,
            max_time=max_time,
            success_rate=success_rate,
            successful_runs=successful_runs,
            failed_runs=failed_runs,
            confidence_interval=conf_interval
        )

    def _display_statistical_results(self, opt_stats: StatisticalResult,
                                   unopt_stats: StatisticalResult,
                                   query_config: Dict[str, Any]):
        """Display comprehensive statistical results"""

        print(f"\n📊 {self.colored_text('STATISTICAL RESULTS', 'bold')}")
        print("=" * 50)

        # Optimized results
        print(f"\n🚀 {self.colored_text('OPTIMIZED APPROACH:', 'green')}")
        if opt_stats.successful_runs > 0:
            print(f"   📈 Mean Time: {opt_stats.mean_time:.2f}ms ± {opt_stats.std_deviation:.2f}ms")
            print(f"   📊 Range: {opt_stats.min_time:.2f}ms - {opt_stats.max_time:.2f}ms")
            print(f"   🎯 95% Confidence: {opt_stats.confidence_interval[0]:.2f}ms - {opt_stats.confidence_interval[1]:.2f}ms")
            print(f"   ✅ Success Rate: {opt_stats.success_rate:.1f}% ({opt_stats.successful_runs}/{opt_stats.run_count})")
        else:
            print(f"   ❌ All runs failed ({opt_stats.failed_runs}/{opt_stats.run_count})")

        # Unoptimized results
        print(f"\n🐌 {self.colored_text('UNOPTIMIZED APPROACH:', 'red')}")
        if unopt_stats.successful_runs > 0:
            print(f"   📈 Mean Time: {unopt_stats.mean_time:.2f}ms ± {unopt_stats.std_deviation:.2f}ms")
            print(f"   📊 Range: {unopt_stats.min_time:.2f}ms - {unopt_stats.max_time:.2f}ms")
            print(f"   🎯 95% Confidence: {unopt_stats.confidence_interval[0]:.2f}ms - {unopt_stats.confidence_interval[1]:.2f}ms")
            print(f"   ✅ Success Rate: {unopt_stats.success_rate:.1f}% ({unopt_stats.successful_runs}/{unopt_stats.run_count})")
        else:
            print(f"   ❌ All runs failed ({unopt_stats.failed_runs}/{unopt_stats.run_count})")

        # Comparative analysis
        if opt_stats.successful_runs > 0 and unopt_stats.successful_runs > 0:
            speedup = unopt_stats.mean_time / opt_stats.mean_time
            improvement = ((unopt_stats.mean_time - opt_stats.mean_time) / unopt_stats.mean_time) * 100
            time_saved = unopt_stats.mean_time - opt_stats.mean_time

            print(f"\n🏆 {self.colored_text('COMPARATIVE ANALYSIS:', 'yellow')}")
            print(f"   ⚡ Mean Speedup: {speedup:.1f}x faster")
            print(f"   📈 Performance Improvement: {improvement:.1f}%")
            print(f"   ⏱️  Average Time Saved: {time_saved:.2f}ms")

            # Statistical significance
            if speedup > 2:
                significance = "Highly Significant"
            elif speedup > 1.5:
                significance = "Significant"
            elif speedup > 1.1:
                significance = "Moderate"
            else:
                significance = "Minimal"

            print(f"   🎯 Statistical Significance: {significance}")

            # Consistency analysis
            opt_cv = (opt_stats.std_deviation / opt_stats.mean_time) * 100 if opt_stats.mean_time > 0 else 0
            unopt_cv = (unopt_stats.std_deviation / unopt_stats.mean_time) * 100 if unopt_stats.mean_time > 0 else 0

            print(f"   📊 Optimized Consistency: {100-opt_cv:.1f}% (CV: {opt_cv:.1f}%)")
            print(f"   📊 Unoptimized Consistency: {100-unopt_cv:.1f}% (CV: {unopt_cv:.1f}%)")

    def _generate_statistical_analysis(self, opt_stats: StatisticalResult,
                                     unopt_stats: StatisticalResult) -> List[str]:
        """Generate detailed statistical analysis"""

        analysis = []

        if opt_stats.successful_runs > 0 and unopt_stats.successful_runs > 0:
            speedup = unopt_stats.mean_time / opt_stats.mean_time

            if speedup > 10:
                analysis.append("🎯 Optimization provides dramatic performance improvement")
                analysis.append("💡 This level of speedup is production-critical")
            elif speedup > 2:
                analysis.append("✅ Optimization provides significant performance benefit")
                analysis.append("💡 Recommended for frequent queries")
            elif speedup > 1.2:
                analysis.append("📊 Optimization provides moderate performance benefit")
            else:
                analysis.append("📝 Both approaches perform similarly")
                analysis.append("💡 Optimization may not be critical for this query pattern")

            # Consistency analysis
            opt_cv = (opt_stats.std_deviation / opt_stats.mean_time) * 100 if opt_stats.mean_time > 0 else 0
            if opt_cv < 10:
                analysis.append("🎯 Optimized approach shows consistent performance")
            elif opt_cv > 25:
                analysis.append("⚠️ Optimized approach shows variable performance")

        elif opt_stats.successful_runs == 0:
            analysis.append("❌ Optimized approach failed consistently")
            analysis.append("💡 Query optimization strategy needs revision")
        elif unopt_stats.successful_runs == 0:
            analysis.append("❌ Unoptimized approach failed consistently")
            analysis.append("💡 Query might be too complex for current dataset")

        return analysis

    def export_results(self, results: Dict[str, Any], format_type: str = 'csv') -> str:
        """Export statistical results to various formats"""

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        if format_type == 'csv':
            filename = f"performance_analysis_{timestamp}.csv"
            self._export_csv(results, filename)
        elif format_type == 'json':
            filename = f"performance_analysis_{timestamp}.json"
            self._export_json(results, filename)
        else:
            raise ValueError(f"Unsupported export format: {format_type}")

        return filename

    def _export_csv(self, results: Dict[str, Any], filename: str):
        """Export results to CSV format"""

        with open(filename, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)

            # Header
            writer.writerow(['Approach', 'Run', 'Execution_Time_ms'])

            # Optimized data
            opt_stats = results['optimized_stats']
            for i, time_ms in enumerate(opt_stats.execution_times, 1):
                writer.writerow(['Optimized', i, time_ms])

            # Unoptimized data
            unopt_stats = results['unoptimized_stats']
            for i, time_ms in enumerate(unopt_stats.execution_times, 1):
                writer.writerow(['Unoptimized', i, time_ms])

    def _export_json(self, results: Dict[str, Any], filename: str):
        """Export results to JSON format"""

        # Convert StatisticalResult objects to dictionaries
        export_data = {
            'query_config': results['query_config'],
            'run_count': results['run_count'],
            'timestamp': results['timestamp'],
            'optimized_stats': {
                'mean_time': results['optimized_stats'].mean_time,
                'std_deviation': results['optimized_stats'].std_deviation,
                'min_time': results['optimized_stats'].min_time,
                'max_time': results['optimized_stats'].max_time,
                'success_rate': results['optimized_stats'].success_rate,
                'execution_times': results['optimized_stats'].execution_times
            },
            'unoptimized_stats': {
                'mean_time': results['unoptimized_stats'].mean_time,
                'std_deviation': results['unoptimized_stats'].std_deviation,
                'min_time': results['unoptimized_stats'].min_time,
                'max_time': results['unoptimized_stats'].max_time,
                'success_rate': results['unoptimized_stats'].success_rate,
                'execution_times': results['unoptimized_stats'].execution_times
            },
            'analysis': results['analysis']
        }

        with open(filename, 'w') as jsonfile:
            json.dump(export_data, jsonfile, indent=2)

    def colored_text(self, text: str, color: str) -> str:
        """Add color to text for better terminal display"""
        colors = {
            'header': '\033[95m',
            'blue': '\033[94m',
            'green': '\033[92m',
            'yellow': '\033[93m',
            'red': '\033[91m',
            'bold': '\033[1m',
            'underline': '\033[4m',
            'end': '\033[0m'
        }
        return f"{colors.get(color, '')}{text}{colors['end']}"
