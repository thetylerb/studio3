import os
from dotenv import load_dotenv
from supabase import create_client, Client
import pandas as pd
import matplotlib.pyplot as plt

def get_client() -> Client:
    load_dotenv()
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    if not url or not key:
        raise RuntimeError("Missing SUPABASE_URL or SUPABASE_KEY in .env")
    return create_client(url, key)

def save_table_image(df: pd.DataFrame, out_path: str, title: str = ""):
    fig, ax = plt.subplots()
    ax.axis("off")
    if title:
        ax.set_title(title, pad=12)
    tbl = ax.table(cellText=df.values, colLabels=df.columns, loc="center")
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(8)
    tbl.scale(1, 1.2)
    plt.tight_layout()
    plt.savefig(out_path, dpi=200)
    plt.close(fig)

def main():
    supabase = get_client()

    # 1) Peek first 5 rows
    peek = supabase.table("ticket_orders").select("*").limit(5).execute()
    print("First 5 rows:")
    for row in peek.data:
        print(row)

    # 2) Average price by fan_tier
    resp_tier = supabase.table("ticket_orders").select("fan_tier, price").execute()
    df_tier = pd.DataFrame(resp_tier.data or [])
    if df_tier.empty:
        print("\nNo data returned. Check RLS or table contents.")
        return

    avg_by_tier = (
        df_tier.groupby("fan_tier", dropna=False)["price"]
        .mean()
        .reset_index(name="avg_price")
        .sort_values("avg_price", ascending=False)
    )
    print("\nAverage price by fan_tier:")
    print(avg_by_tier)
    save_table_image(avg_by_tier, "avg_price_by_tier.png",
                     "Average Ticket Price by Fan Tier")

    # 3) Total revenue by event & purchase_channel
    resp_rev = supabase.table("ticket_orders").select("event, purchase_channel, price").execute()
    df_rev = pd.DataFrame(resp_rev.data or [])
    if not df_rev.empty:
        revenue = (
            df_rev.groupby(["event", "purchase_channel"])["price"]
            .sum()
            .reset_index(name="total_revenue")
            .sort_values(["event", "total_revenue"], ascending=[True, False])
        )
        print("\nTotal revenue by event & purchase_channel:")
        print(revenue)
        save_table_image(revenue, "total_revenue_event_channel.png",
                         "Revenue by Event & Purchase Channel")

if __name__ == "__main__":
    main()
