# -*- coding: utf-8 -*-
"""
Affinity Reporter Module
Generates reports and visualizations for market basket analysis results.
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from typing import Optional


class AffinityReporter:
    """Generates reports and visualizations for affinity analysis."""
    
    @staticmethod
    def generate_affinity_report(
        results_df: pd.DataFrame,
        products_df: Optional[pd.DataFrame] = None,
        top_n: int = 10
    ) -> str:
        """
        Generate a text-based affinity report.
        
        Args:
            results_df (pd.DataFrame): Analysis results
            products_df (pd.DataFrame): Products data
            top_n (int): Number of top affinities to include
            
        Returns:
            str: Formatted report
        """
        report = []
        report.append("=" * 80)
        report.append("SHOPPING BASKET AFFINITY ANALYSIS REPORT")
        report.append("=" * 80)
        report.append("")
        
        # Summary statistics
        report.append("SUMMARY STATISTICS")
        report.append("-" * 80)
        report.append(f"Total association rules found: {len(results_df)}")
        report.append(f"Average support: {results_df['support'].mean():.4f}")
        report.append(f"Average confidence: {results_df['confidence'].mean():.4f}")
        report.append(f"Average lift: {results_df['lift'].mean():.4f}")
        report.append("")
        
        # Top affinities
        report.append(f"TOP {top_n} PRODUCT AFFINITIES (by Lift)")
        report.append("-" * 80)
        
        top_affinities = results_df.nlargest(top_n, 'lift')
        
        for idx, row in top_affinities.iterrows():
            if 'product_a_name' in row and 'product_b_name' in row:
                product_a = f"{row['product_a_name']} ({row['product_a']})"
                product_b = f"{row['product_b_name']} ({row['product_b']})"
            else:
                product_a = row['product_a']
                product_b = row['product_b']
            
            report.append(f"\n{idx + 1}. {product_a} → {product_b}")
            report.append(f"   Support:    {row['support']:.4f}")
            report.append(f"   Confidence: {row['confidence']:.4f}")
            report.append(f"   Lift:       {row['lift']:.4f}")
        
        report.append("")
        report.append("=" * 80)
        
        return "\n".join(report)
    
    @staticmethod
    def export_recommendations(
        results_df: pd.DataFrame,
        output_file: str = 'recommendations.csv'
    ):
        """
        Export recommendations to CSV.
        
        Args:
            results_df (pd.DataFrame): Analysis results
            output_file (str): Output file path
        """
        # Create "Customers who buy X also buy Y" format
        recommendations = results_df.copy()
        
        if 'product_a_name' in recommendations.columns:
            recommendations['recommendation'] = (
                "Customers who buy " + 
                recommendations['product_a_name'] + 
                " also buy " + 
                recommendations['product_b_name']
            )
        else:
            recommendations['recommendation'] = (
                "Customers who buy " + 
                recommendations['product_a'] + 
                " also buy " + 
                recommendations['product_b']
            )
        
        # Select relevant columns
        export_cols = ['recommendation', 'support', 'confidence', 'lift']
        if 'product_a_name' in recommendations.columns:
            export_cols = ['product_a', 'product_a_name', 'product_b', 'product_b_name'] + export_cols
        
        recommendations[export_cols].to_csv(output_file, index=False, encoding='utf-8')
        print(f"  ✓ Recommendations exported to {output_file}")
    
    @staticmethod
    def create_affinity_heatmap(
        results_df: pd.DataFrame,
        products_df: Optional[pd.DataFrame] = None,
        metric: str = 'lift',
        output_file: str = 'affinity_heatmap.png'
    ):
        """
        Create an affinity heatmap visualization.
        
        Args:
            results_df (pd.DataFrame): Analysis results
            products_df (pd.DataFrame): Products data
            metric (str): Metric to visualize ('lift', 'confidence', 'support')
            output_file (str): Output file path
        """
        # Create pivot table
        pivot = results_df.pivot_table(
            index='product_a',
            columns='product_b',
            values=metric,
            fill_value=0
        )
        
        # Create heatmap
        plt.figure(figsize=(12, 10))
        sns.heatmap(
            pivot,
            annot=True,
            fmt='.2f',
            cmap='YlOrRd',
            cbar_kws={'label': metric.capitalize()},
            linewidths=0.5
        )
        
        plt.title(f'Product Affinity Heatmap ({metric.capitalize()})', fontsize=16, fontweight='bold')
        plt.xlabel('Product B', fontsize=12)
        plt.ylabel('Product A', fontsize=12)
        plt.tight_layout()
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"  ✓ Heatmap saved to {output_file}")
    
    @staticmethod
    def create_top_affinities_chart(
        results_df: pd.DataFrame,
        top_n: int = 10,
        output_file: str = 'top_affinities.png'
    ):
        """
        Create a bar chart of top affinities.
        
        Args:
            results_df (pd.DataFrame): Analysis results
            top_n (int): Number of top affinities to show
            output_file (str): Output file path
        """
        top_affinities = results_df.nlargest(top_n, 'lift')
        
        # Create labels
        if 'product_a_name' in top_affinities.columns:
            labels = (
                top_affinities['product_a_name'] + 
                ' → ' + 
                top_affinities['product_b_name']
            )
        else:
            labels = (
                top_affinities['product_a'] + 
                ' → ' + 
                top_affinities['product_b']
            )
        
        # Create figure with subplots
        fig, axes = plt.subplots(1, 3, figsize=(18, 6))
        
        # Lift chart
        axes[0].barh(range(len(top_affinities)), top_affinities['lift'], color='#FF6B6B')
        axes[0].set_yticks(range(len(top_affinities)))
        axes[0].set_yticklabels(labels, fontsize=9)
        axes[0].set_xlabel('Lift', fontsize=11)
        axes[0].set_title('Top Affinities by Lift', fontsize=12, fontweight='bold')
        axes[0].invert_yaxis()
        
        # Confidence chart
        axes[1].barh(range(len(top_affinities)), top_affinities['confidence'], color='#4ECDC4')
        axes[1].set_yticks(range(len(top_affinities)))
        axes[1].set_yticklabels(labels, fontsize=9)
        axes[1].set_xlabel('Confidence', fontsize=11)
        axes[1].set_title('Top Affinities by Confidence', fontsize=12, fontweight='bold')
        axes[1].invert_yaxis()
        
        # Support chart
        axes[2].barh(range(len(top_affinities)), top_affinities['support'], color='#95E1D3')
        axes[2].set_yticks(range(len(top_affinities)))
        axes[2].set_yticklabels(labels, fontsize=9)
        axes[2].set_xlabel('Support', fontsize=11)
        axes[2].set_title('Top Affinities by Support', fontsize=12, fontweight='bold')
        axes[2].invert_yaxis()
        
        plt.tight_layout()
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"  ✓ Top affinities chart saved to {output_file}")
    
    @staticmethod
    def print_summary(results_df: pd.DataFrame, top_n: int = 10):
        """
        Print a summary of affinity analysis to console.
        
        Args:
            results_df (pd.DataFrame): Analysis results
            top_n (int): Number of top affinities to show
        """
        print("\n" + "=" * 80)
        print("AFFINITY ANALYSIS SUMMARY")
        print("=" * 80)
        
        print(f"\nTotal association rules: {len(results_df)}")
        print(f"Average lift: {results_df['lift'].mean():.4f}")
        print(f"Max lift: {results_df['lift'].max():.4f}")
        
        print(f"\nTop {top_n} Product Affinities:")
        print("-" * 80)
        
        top = results_df.nlargest(top_n, 'lift')
        
        for idx, row in top.iterrows():
            if 'product_a_name' in row:
                print(f"\n{idx + 1}. {row['product_a_name']} → {row['product_b_name']}")
            else:
                print(f"\n{idx + 1}. {row['product_a']} → {row['product_b']}")
            
            print(f"   Lift: {row['lift']:.4f} | Confidence: {row['confidence']:.4f} | Support: {row['support']:.4f}")
        
        print("\n" + "=" * 80)


def generate_complete_report(
    results_df: pd.DataFrame,
    products_df: Optional[pd.DataFrame] = None,
    output_dir: str = '.',
    top_n: int = 10
):
    """
    Generate complete affinity analysis report with all visualizations.
    
    Args:
        results_df (pd.DataFrame): Analysis results
        products_df (pd.DataFrame): Products data
        output_dir (str): Output directory
        top_n (int): Number of top affinities
    """
    reporter = AffinityReporter()
    
    # Print summary
    reporter.print_summary(results_df, top_n)
    
    # Generate text report
    report_text = reporter.generate_affinity_report(results_df, products_df, top_n)
    with open(f'{output_dir}/affinity_report.txt', 'w', encoding='utf-8') as f:
        f.write(report_text)
    print(f"\n  ✓ Text report saved to {output_dir}/affinity_report.txt")
    
    # Export recommendations
    reporter.export_recommendations(results_df, f'{output_dir}/recommendations.csv')
    
    # Create visualizations
    try:
        reporter.create_top_affinities_chart(results_df, top_n, f'{output_dir}/top_affinities.png')
        reporter.create_affinity_heatmap(results_df, products_df, 'lift', f'{output_dir}/affinity_heatmap.png')
    except Exception as e:
        print(f"\n  ⚠ Could not create visualizations: {e}")
        print("    (matplotlib/seaborn may not be installed)")
