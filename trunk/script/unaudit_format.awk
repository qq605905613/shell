# 使用说明
# awk -f audited_format.awk auditedMchnt.csv

# 设置输入、输出文件分隔符
BEGIN{
    FS=","
    OFS=","
}

# 跳过第一行标题行
NR == 1{next}

# 将timestamp为空字符的列替换为null 
{
    $12 = $12 ~ /^""$/ ? "" : $12
    $13 = $13 ~ /^""$/ ? "" : $13
    $15 = $15 ~ /^""$/ ? "" : $15
    print
    #print $12,$13,$15
}

END{
    #print FS
}
