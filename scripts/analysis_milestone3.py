import pandas as pd
import matplotlib.pyplot as plt
import os

def milestone3_analysis():
    # Datasets and mixed workload suffixes
    datasets = ['fb', 'osmc', 'books']
    workloads = {
        '10pct_lookup': '0.100000i_0m_mix',  # 10% lookup, 90% insert
        '90pct_lookup': '0.900000i_0m_mix'   # 90% lookup, 10% insert
    }
    indexes = ['DynamicPGM', 'LIPP', 'HybridPGMLipp']

    # Create output directory
    os.makedirs('analysis_results', exist_ok=True)

    for wname, suffix in workloads.items():
        # Set up a 2×3 grid: row 0 = throughput, row 1 = index size
        fig, axs = plt.subplots(2, len(datasets), figsize=(4*len(datasets), 8))

        for col, ds in enumerate(datasets):
            # Construct filename
            fname = (
                f"results/{ds}_100M_public_uint64_ops_2M_0.000000rq_0.500000nl_"
                f"{suffix}_results_table.csv"
            )
            # Load data
            df = pd.read_csv(fname)

            # Extract throughput and size for each index
            throughput = []
            size_bytes = []
            for ix in indexes:
                sub = df[df['index_name'] == ix]
                # average the three mixed throughput measurements
                tp_cols = [c for c in sub.columns if 'mixed_throughput_mops' in c]
                avg_tp = sub[tp_cols].mean(axis=1).max() if not sub.empty else 0
                throughput.append(avg_tp)

                # index_size_bytes column
                if 'index_size_bytes' in sub.columns and not sub.empty:
                    size_bytes.append(sub['index_size_bytes'].iloc[0])
                else:
                    size_bytes.append(0)

            # Plot throughput
            ax = axs[0, col]
            ax.bar(indexes, throughput)
            ax.set_title(f"{ds} — Throughput")
            ax.set_ylabel('Mops/s')
            ax.set_ylim(0, max(throughput) * 1.2 if any(throughput) else 1)

            # Plot index size
            ax = axs[1, col]
            ax.bar(indexes, size_bytes)
            ax.set_title(f"{ds} — Index Size")
            ax.set_ylabel('Bytes')
            ax.set_ylim(0, max(size_bytes) * 1.2 if any(size_bytes) else 1)

        fig.suptitle(f"Mixed Workload: {wname.replace('_', ' ')}", fontsize=16)
        plt.tight_layout(rect=[0, 0, 1, 0.95])
        out_png = f"analysis_results/{wname}.png"
        fig.savefig(out_png, dpi=300)
        plt.close(fig)
        print(f"Saved plot: {out_png}")

if __name__ == '__main__':
    milestone3_analysis()
