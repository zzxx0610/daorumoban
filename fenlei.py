import streamlit as st
import pandas as pd
import os
import io
import zipfile
import time
import datetime

# --- 页面基础设置 ---
st.set_page_config(
    page_title="数据转换与拆分工具",
    page_icon="✨",
    layout="wide"
)

# --- 目标表格的字段列表 ---
TARGET_COLUMNS = [
    '装货单编号', '卸货单编号', '司机', '手机号', '车牌号', '挂车', 
    '装车量(吨)', '卸货量(吨)', '装车时间', '卸货时间', '司机运费单价', 
    '发货单位名称', '发货单位证件号', '发货点简称', '发货(省)', '发货(市)', 
    '发货(区)', '发货详细地址', '发货联系人', '发货联系人电话', 
    '收货单位名称', '收货单位证件号', '收获地址简称', '收货(省)', '收货(市)', 
    '收货(区)', '收货详细地址', '收货联系人', '收货联系人电话'
]

def transform_and_process(uploaded_file, group_by_column, log_container):
    """
    读取、转换、拆分并打包Excel文件。
    """
    logs = []
    def log_message(message):
        logs.append(message)
        log_container.markdown("```\n" + "\n".join(logs) + "\n```")

    try:
        source_filename = os.path.splitext(uploaded_file.name)[0]
        log_message(f"准备处理文件: {uploaded_file.name}")
        
        df_source = pd.read_excel(uploaded_file)
        total_rows = len(df_source)
        log_message(f"✅ 成功读取源文件，共包含 {total_rows} 条数据。")

        log_message("⏳ 开始进行数据结构转换...")
        
        new_rows = []
        for index, row in df_source.iterrows():
            new_row = {}
            # 字段映射
            new_row['司机'] = row.get('司机姓名（收款人）')
            new_row['手机号'] = row.get('司机手机号码（收款人）')
            new_row['车牌号'] = row.get('车牌')
            
            # 时间字段格式化处理
            for col_name in ['装车时间', '卸货时间']:
                time_val = row.get(col_name)
                if pd.notna(time_val) and isinstance(time_val, (pd.Timestamp, datetime.datetime)):
                    new_row[col_name] = time_val.strftime('%Y/%m/%d %H:%M')
                else:
                    new_row[col_name] = time_val
            
            # ==================== 新增的映射规则 ====================
            # 从源表获取“货主名称”
            货主名称_val = row.get('货主名称')
            # 将其同时赋值给目标表的三个字段
            new_row['收货单位名称'] = 货主名称_val
            new_row['收获地址简称'] = 货主名称_val
            new_row['收货联系人'] = 货主名称_val
            # =========================================================

            # 数量映射
            装车量 = row.get('司机装货数量')
            new_row['装车量(吨)'] = 装车量
            new_row['卸货量(吨)'] = 装车量
            
            # 计算字段
            里程 = pd.to_numeric(row.get('里程'), errors='coerce')
            单价 = pd.to_numeric(row.get('司机运输单价（人民币）'), errors='coerce')
            if pd.notna(里程) and pd.notna(单价):
                new_row['司机运费单价'] = 里程 * 单价
            else:
                new_row['司机运费单价'] = None
            
            new_row[group_by_column] = row.get(group_by_column)
            new_rows.append(new_row)
        
        df_target = pd.DataFrame(new_rows)
        df_target = df_target.reindex(columns=TARGET_COLUMNS + [group_by_column])

        log_message("✅ 数据结构转换完成！")
        log_message("-" * 40)
        
        unique_groups = df_source[group_by_column].dropna().unique()
        log_message(f"🔍 在“{group_by_column}”列中发现 {len(unique_groups)} 个独立的项目，准备开始拆分...")
        
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
            for i, group_value in enumerate(unique_groups, 1):
                source_group_df = df_source[df_source[group_by_column] == group_value]
                source_group_rows = len(source_group_df)
                
                target_group_df = df_target[df_target[group_by_column] == group_value]
                final_df_to_save = target_group_df.drop(columns=[group_by_column])

                safe_filename = "".join([c for c in str(group_value) if c.isalnum() or c in (' ', '_', '-')]).rstrip()
                if not safe_filename:
                    safe_filename = f"未命名区域_{i}"
                
                output_filename_in_zip = f"{safe_filename}.xlsx"
                
                excel_buffer = io.BytesIO()
                final_df_to_save.to_excel(excel_buffer, index=False, engine='openpyxl')
                excel_buffer.seek(0)
                
                zf.writestr(output_filename_in_zip, excel_buffer.read())
                
                log_message(f"({i}/{len(unique_groups)}) 已生成文件: {output_filename_in_zip} (源表行数: {source_group_rows}, 新表行数: {len(final_df_to_save)})")
                time.sleep(0.01)

        log_message("-" * 40)
        log_message("✅ 所有表格拆分完成！")
        zip_buffer.seek(0)
        return zip_buffer, source_filename

    except Exception as e:
        st.error(f"处理过程中发生严重错误: {e}")
        log_message(f"❌ 错误详情: {e}")
        return None, None

# --- Streamlit 界面布局 (这部分保持不变) ---
st.title("✨ 表格数据转换与按区域拆分工具")
st.markdown("""
上传一个特定格式的源Excel表，工具将：
1.  按照预设规则**转换数据结构**。
2.  根据 **“区域”** 字段对数据进行分类。
3.  为每个区域生成一个独立的Excel文件，并打包成ZIP供您下载。
""")
st.markdown("---")

uploaded_file = st.file_uploader("上传您的源数据 Excel 表", type=['xlsx'])

if uploaded_file is not None:
    st.subheader("1. 确认拆分规则")
    
    group_by_column_fixed = "区域"
    st.info(f"本工具将默认根据 **`{group_by_column_fixed}`** 列进行拆分。")

    st.subheader("2. 开始处理并查看日志")
    log_container = st.empty()
    log_container.info("准备就绪，点击下方按钮开始处理。")

    if st.button("🚀 开始转换并拆分", use_container_width=True):
        log_container.empty()
        
        zip_buffer, source_filename = transform_and_process(uploaded_file, group_by_column_fixed, log_container)
        
        if zip_buffer and source_filename:
            st.success("🎉 处理完成！可以下载结果了。")
            
            st.subheader("3. 下载结果")
            st.download_button(
                label="📥 下载转换后的结果 (ZIP)",
                data=zip_buffer,
                file_name=f'{source_filename}_按区域拆分.zip',
                mime='application/zip',
                use_container_width=True
            )
else:
    st.info("请上传一个 .xlsx 文件以开始。")

st.markdown("---")
st.write("由 AI 与开发者共同构建的定制化工具。")
