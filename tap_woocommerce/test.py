from hotglue_singer_sdk import typing as th

th.Property(
"line_items",
th.ArrayType(
    th.ObjectType(
        th.Property("id", th.IntegerType),
        th.Property("name", th.StringType),
        th.Property("product_id", th.IntegerType),
        th.Property("variation_id", th.IntegerType),
        th.Property("quantity", th.IntegerType),
        th.Property("tax_class", th.StringType),
        th.Property("subtotal", th.StringType),
        th.Property("subtotal_tax", th.StringType),
        th.Property("total", th.StringType),
        th.Property("total_tax", th.StringType),
        th.Property(
            "taxes",
            th.ArrayType(
                th.ObjectType(
                    th.Property("id", th.IntegerType),
                    th.Property("rate_code", th.StringType),
                    th.Property("rate_id", th.StringType),
                    th.Property("label", th.StringType),
                    th.Property("compound", th.BooleanType),
                    th.Property("tax_total", th.StringType),
                    th.Property("shipping_tax_total", th.StringType),
                    th.Property(
                        "meta_data",
                        th.ArrayType(
                            th.ObjectType(
                                th.Property("id", th.IntegerType),
                                th.Property("key", th.StringType),
                                th.Property("value", th.StringType),
                            ),
                        ),
                    ),
                )
            ),
        ),
    th.Property(
            "meta_data",
            th.ArrayType(
                th.ObjectType(
                    th.Property("id", th.IntegerType),
                    th.Property("key", th.StringType),
                    th.Property("value", th.StringType),
                )
            ),
        ),
        th.Property("sku", th.StringType),
        th.Property("price", th.StringType),
    ),
),
),
