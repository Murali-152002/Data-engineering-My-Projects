"""
Mock internal 'Orders API' - stands in for the kind of upstream REST API a
real ADF pipeline hits via a Web/HTTP Copy Activity. Supports cursor pagination
and a `modified_since` watermark filter, matching real enterprise API contracts.

Run: python3 mock_orders_api.py
"""
from flask import Flask, request, jsonify
import data_store

app = Flask(__name__)


@app.route("/api/orders", methods=["GET"])
def get_orders():
    modified_since = request.args.get("modified_since")
    cursor = int(request.args.get("cursor", 0))
    page_size = int(request.args.get("page_size", 200))
    page, next_cursor, total_matching = data_store.query(modified_since, cursor, page_size)
    return jsonify({
        "data": page,
        "next_cursor": next_cursor,
        "total_matching": total_matching,
        "page_size": page_size,
    })


@app.route("/api/orders/_seed", methods=["POST"])
def seed():
    n = data_store.seed_history()
    return jsonify({"seeded": n})


@app.route("/api/orders/_new_batch", methods=["POST"])
def new_batch():
    n = int(request.args.get("n", 150))
    added, updated = data_store.add_new_batch(n)
    return jsonify({"added": added, "updated": updated})


if __name__ == "__main__":
    data_store.seed_history()
    app.run(host="127.0.0.1", port=5055)
