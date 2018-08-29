options(java.parameters = "-Xmx8192m")
library('data.table')
library('dplyr')
library('reshape2')
library('xlsx')

#Input coefficients
source("/xgm/R_scripts/monthly_realloc/coef.R")

#This function compute exact product shipments from A to B.
compute_prodshipment <- function(sd_from, sd_to, sdcate_from, sdcate_to, min_div = min_prodship){
  temp_dt = data.table(row_num = 1:length(sd_from), sd_from, sd_to)
  temp_dt$rawship = 0
  
  if(sum(sd_from>0 & sd_to<0) > 0){
    temp_dt$rawship[sd_from>0 & sd_to<0] = temp_dt[sd_from>0 & sd_to<0, .(ship = min(abs(sd_from), abs(sd_to))), by = 'row_num']$ship
  }
  
  prodship = temp_dt$rawship
  cateship = apply(cbind(compute_cateshipment(prodship), abs(sdcate_from), abs(sdcate_to)), 1, min)
  
  exact_ship = rep(0, length(sd_from))
  for(k in which(cateship > 0)){
    exact_ship[cate_idx_list[[k]]] = makeup_ship(prodship[cate_idx_list[[k]]], cateship[k])
  }
  
  idx = which(exact_ship < min_div)
  exact_ship[idx] = 0
  return(exact_ship)
}

#This function turns product shipments into category shipments from A to B.
compute_cateshipment <- function(ship_in){
  out_vct = rep(NA, length(cate_idx_list))
  for(k in 1:length(cate_idx_list)){
    out_vct[k] = sum(ship_in[cate_idx_list[[k]]])
  }
  return(out_vct)
}

#This function will change a product shipment vector with a limit sum.
makeup_ship <- function(vct_in, num_in){
  if(sum(vct_in) <= num_in){
    return(vct_in)
  }
  
  vct_out = rep(0, length(vct_in))
  need_prod = which(cumsum(sort(vct_in, decreasing = T)) >= num_in)[1]
  need_idx = order(vct_in, decreasing = T)[1:need_prod]
  
  vct_out[need_idx] = vct_in[need_idx]
  vct_out[need_idx][need_prod] = vct_in[need_idx][need_prod] - (sum(vct_in[need_idx]) - num_in)
  return(vct_out)
}

#This function updates sdmatx with a result_table.
updt_sdmatx <- function(in_matx, result_dt){
  
  nextship_dt = result_dt[dim(result_dt)[1],]
  out_matx = in_matx
  nextship_vct = as.numeric(nextship_dt[, -(1:4)])
  out_matx[,nextship_dt$from] = out_matx[,nextship_dt$from] - nextship_vct
  out_matx[,nextship_dt$to] = out_matx[,nextship_dt$to] + nextship_vct
  if(sum(result_dt$from == nextship_dt$from) == 10){
    idx = which(out_matx[,nextship_dt$from] > 0)
    out_matx[,nextship_dt$from][idx] = 0
  }
  if(sum(result_dt$to == nextship_dt$to) == 10){
    idx = which(out_matx[,nextship_dt$to] < 0)
    out_matx[,nextship_dt$to][idx] = 0
  }
  
  return(out_matx)
}

#This function updates cate_sdmatx with a result_table.
updt_cate_sdmatx <- function(in_matx, result_dt){
  
  nextship_dt = result_dt[dim(result_dt)[1],]
  out_matx = in_matx
  nextship_vct = as.numeric(nextship_dt[, -(1:4)])
  nextcateship_vct = compute_cateshipment(nextship_vct)
  
  out_matx[, nextship_dt$from] = out_matx[, nextship_dt$from] - nextcateship_vct
  out_matx[, nextship_dt$to] = out_matx[, nextship_dt$to] + nextcateship_vct
  
  return(out_matx)
}





#Load input data.
businv = fread(file = '/xgm/R_scripts/monthly_realloc/input/businv.csv', colClasses = c('character', 'character', 'character', 'numeric'))
prod_info = fread(file = '/xgm/R_scripts/monthly_realloc/input/prod_info.csv', colClasses = rep('character', 6), encoding = 'UTF-8')
saleshist = fread(file = '/xgm/R_scripts/monthly_realloc/input/saleshist.csv', colClasses = c('character', 'character','character', 'numeric'))
stopdist = fread(file = '/xgm/R_scripts/monthly_realloc/input/stopdist.csv', colClasses = c('character', 'character'))
store_info = fread(file = '/xgm/R_scripts/monthly_realloc/input/store_info.csv', colClasses = rep('character', 3), encoding = 'UTF-8')
jxcsdrpt = fread(file = '/xgm/R_scripts/monthly_realloc/input/jxcsdrpt.csv', colClasses = c('character', 'character', 'character','numeric'))
storegroup = fread(file = '/xgm/R_scripts/monthly_realloc/input/storegroup.csv',colClasses = c('character', 'character', 'character', 'character', 'character',
                                                                                     'character', 'numeric', 'character', 'character', 'numeric',  'character'))

store_info = store_info %>% left_join(storegroup, by = 'storecode') %>% data.table()
store_info = store_info[!is.na(group), .(storegid, storecode, storename = storename.x, group = group_cn)]



#Loop over all unique groups.
unik_group = sort(unique(store_info$group))


table_1 = data.table(db_group = character(),storecode = character(),storename = character(),tot_inv = numeric(),tot_day = numeric(),tot_sale = numeric(),str_expday = numeric())
table_2 = data.table(db_group = character(),catecode = character(),catename = character(),saleqty = numeric(),invqty = numeric(),cate_expday = numeric())

table_3 = data.table(db_group = character(),storecode = character(),storename = character(),tot_day = numeric(),catecode = character(),catename = character(),
                     str_expday = numeric(),cate_expday = numeric(),sc_rsnexpday = character(), cate_invqty = numeric(), saleqty = numeric(), cate_daysale = numeric(), sc_actexpday = character())

    
table_4 = data.table(db_group = character(),storecode = character(),storename = character(),tot_day = numeric(),catecode = character(),catename = character(),prodcode = character(), oldcode = character(),
                     prodname = character(),prod_invqty = numeric(),prod_saleqty = numeric(),prod_daysale = numeric(),sp_expday = character(), cate_invqty = numeric(),cate_saleqty = numeric(), cate_daysale = numeric(),sc_actexpday = character(),
                     sc_rsnexpday = character(),prodamt = numeric(),cateamt = numeric())

table_5 = data.table(db_group = character(),storecode = character(),storename = character(),tot_day = numeric(),catecode = character(),catename = character(),prodcode = character(), oldcode = character(),
                     prodname = character(),prod_invqty = numeric(),prod_saleqty = numeric(),prod_daysale = numeric(),sp_expday = character(), cate_invqty = numeric(),cate_saleqty = numeric(), cate_daysale = numeric(),sc_actexpday = character(),
                     sc_rsnexpday = character(),prodamt = numeric(),cateamt = numeric())

table_6 = data.table(db_group = character(),from_storecode = character(),from_strorename = character(),to_storecode = character(),to_storename = character(),
                     amt_ship = numeric(),fill_pct = numeric(),tot_amt = numeric(),tot_pct = numeric())



table_7 = data.table(db_group = character(),shipment_no=numeric(),from_storecode = character(),from_strorename = character(),to_storecode = character(),to_storename = character(),
                     prodcode = character(),oldcode = character(),prodname = character(),shipment_amt = numeric())



length(unik_group)
for(grp in 1:length(unik_group)){
  
  idx = which(store_info$group == unik_group[grp])
  loop_str = store_info$storecode[idx]
  
  #Get inv and sales for only the stores in the group.
  loop_businv = businv[storecode %in% loop_str]
  loop_saleshist = saleshist[storecode %in% loop_str]
  loop_jxcsdrpt = jxcsdrpt[storecode %in% loop_str]
  
  #Get store total sales info.
  str_sumy = store_info[storecode %in% loop_str, .(storecode, storename)] %>% 
    left_join(loop_businv[,.(tot_inv = sum(qty)), by = storecode], by = 'storecode') %>%
    left_join(loop_saleshist[,.(tot_day = length(unique(date)) , tot_sale = sum(qty)), by = storecode], by = 'storecode') %>%
    data.table()
  
  #Drop stores without any sales record.
  str_sumy = str_sumy[!is.na(tot_day)]
  str_sumy$str_expday = round(str_sumy$tot_inv/(str_sumy$tot_sale/str_sumy$tot_day),2)
  str_sumy = str_sumy[order(storecode)]
  grp_expday = round(sum(str_sumy$tot_inv)/(sum(str_sumy$tot_sale)/30),2)
  

  #Write str_sumy.
  var_write1 = rbind(str_sumy, data.table(storecode = '小组', storename='小组门店', tot_inv = sum(str_sumy$tot_inv), tot_day = 30, tot_sale = sum(str_sumy$tot_sale), str_expday = grp_expday))
  var_write1 = data.table(db_group = unik_group[grp], var_write1)
  table_1 = rbind(table_1, var_write1)
  
  
  #Get cate total sales info.
  cate_sumy = loop_saleshist[,.(prodqty = sum(qty)), by = prodcode] %>% left_join(prod_info, by = 'prodcode') %>% data.table()
  cate_sumy = cate_sumy[which(!is.na(cate_sumy$catecode)),]
  cate_sumy = cate_sumy[,.(saleqty = sum(prodqty)), by = .(catecode, catename)]
  cate_sumy = cate_sumy[order(catecode)]
  cate_sumy = cate_sumy %>% left_join(loop_businv[,.(invqty = sum(qty)), by = catecode], by = 'catecode') %>% data.table()
  cate_sumy$cate_expday = round(cate_sumy$invqty/(cate_sumy$saleqty/30),2)
  
  cate_sumy$invqty[is.na(cate_sumy$invqty)] = 0
  cate_sumy$cate_expday[is.na(cate_sumy$cate_expday)] = 0
  

  #Write cate_sumy.
  var_write2 = cate_sumy
  var_write2 = data.table(db_group = unik_group[grp], var_write2)
  
  if(dim(var_write2)[1]>0){
    table_2 = rbind(table_2, var_write2)
  }
  

  
  
  
  #Get store cate resonable expday.
  sc_sumy = data.table(expand.grid(storecode = str_sumy$storecode, catecode = cate_sumy$catecode, stringsAsFactors = F)) %>%
    left_join(str_sumy[,.(storecode, storename, str_expday)], by = 'storecode') %>%
    left_join(cate_sumy[,.(catecode, catename, cate_expday)], by = 'catecode') %>%
    data.table()

  sc_sumy$sc_rsnexpday = round(sc_sumy$str_expday*sc_sumy$cate_expday/grp_expday,2)

  #Get store cate resonable & actual expday.
  dummyvar = loop_saleshist[,.(qty = sum(qty)), by = .(storecode, prodcode)] %>% left_join(prod_info[,.(prodcode, catecode)], by = 'prodcode') %>% data.table()
  dummyvar = dummyvar[!is.na(catecode)]
  dummyvar = dummyvar[,.(saleqty = sum(qty)), by = .(storecode, catecode)]

  dummyvar2 = loop_businv[,.(invqty = sum(qty)), by = .(storecode, catecode)]
  dummyvar2 = dummyvar2[catecode %in% unique(prod_info$catecode)]
  
  dummyvar3 = data.table(expand.grid(storecode = str_sumy$storecode,catecode = cate_sumy$catecode, stringsAsFactors = F)) %>%
    left_join(dummyvar[,.(storecode, catecode, saleqty)], by = c('storecode', 'catecode')) %>%
    left_join(dummyvar2[,.(storecode, catecode, invqty)], by = c('storecode', 'catecode')) %>%
    left_join(str_sumy[,.(storecode, tot_day)], by = 'storecode') %>%
    data.table()
  
  dummyvar3$saleqty[which(is.na(dummyvar3$saleqty))] = 0
  dummyvar3$invqty[which(is.na(dummyvar3$invqty))] = 0
  dummyvar3$cate_daysale = round(dummyvar3$saleqty/dummyvar3$tot_day,5)
  dummyvar3$sc_actexpday = round(dummyvar3$invqty/dummyvar3$cate_daysale,2)
  sc_sumy = sc_sumy %>% 
    left_join(dummyvar3[,.(storecode, catecode, cate_invqty = invqty, saleqty, tot_day, cate_daysale, sc_actexpday)], by = c('storecode', 'catecode')) %>%
    data.table()
  
  sc_sumy = sc_sumy[!is.na(sc_actexpday)]

  
  #Write sc_sumy.
  var_write3 = sc_sumy[,.(storecode, storename, tot_day, catecode, catename, str_expday, cate_expday, sc_rsnexpday, cate_invqty, saleqty, cate_daysale, sc_actexpday)]
  var_write3$sc_rsnexpday[which(var_write3$sc_rsnexpday == Inf)] = "不动销"
  var_write3$sc_actexpday[which(var_write3$sc_actexpday == Inf)] = "不动销"
  
  var_write3 = var_write3[order(storecode, catecode)]
  var_write3 = data.table(db_group = unik_group[grp], var_write3)
  
  if(dim(var_write3)[1]>0){
    table_3 = rbind(table_3, var_write3)
  }
  
  
  
  
  #Get store prod resonable & actual expday.
  sp_sumy = data.table(expand.grid(storecode = str_sumy$storecode, prodcode = prod_info$prodcode, stringsAsFactors = F)) %>% 
    left_join(loop_saleshist[,.(saleqty = sum(qty)), by = .(storecode, prodcode)], by = c('storecode','prodcode')) %>%
    left_join(loop_businv[,.(storecode, prodcode, prod_invqty = qty)], by = c('storecode','prodcode')) %>%
    left_join(str_sumy[,.(storecode,storename, tot_day)], by = 'storecode') %>%
    left_join(prod_info[,.(prodcode, oldcode, prodname, catecode, catename)], by = 'prodcode') %>%
    data.table()

  sp_sumy$saleqty[which(is.na(sp_sumy$saleqty))] = 0
  sp_sumy$prod_invqty[which(is.na(sp_sumy$prod_invqty))] = 0
  
  sp_sumy$prod_daysale = round(sp_sumy$saleqty/sp_sumy$tot_day,5)
  sp_sumy$sp_expday = round(sp_sumy$prod_invqty/sp_sumy$prod_daysale,2)
  
  #Get rid of 0/0, zero sales and inventory.
  sp_sumy = sp_sumy[!is.na(sp_expday)]
  sp_sumy = sp_sumy[,.(storecode, storename, catecode, catename, prodcode, oldcode, prodname, prod_invqty, prod_saleqty = saleqty, tot_day, prod_daysale, sp_expday)]

  #Combine store product and category info.
  spc_all = sp_sumy %>% left_join(sc_sumy[,.(storecode, catecode, sc_rsnexpday, cate_invqty, cate_saleqty = saleqty, cate_daysale, sc_actexpday)], by = c('storecode','catecode')) %>% data.table()
  
  #Compute out-store info.
  spc_out = spc_all[sp_expday > 40 & sc_actexpday > 40 & prod_invqty > 0]
  spc_out$cateamt = round((spc_out$sc_actexpday - pmax(spc_out$sc_rsnexpday, 40)) * spc_out$cate_daysale)
  
  idx = which(spc_out$sc_actexpday == Inf)
  spc_out$cateamt[idx]= spc_out$cate_invqty[idx]

  spc_out = spc_out[cateamt > 0]
  spc_out$cateamt = pmin(spc_out$cateamt, spc_out$cate_invqty)

  spc_out$prodamt = round(pmax(spc_out$sp_expday-40,0) * spc_out$prod_daysale)
  
  idx = which(spc_out$sp_expday == Inf)
  spc_out$prodamt[idx] = spc_out$prod_invqty[idx]
  spc_out$prodamt = pmin(spc_out$prodamt, spc_out$cateamt)
  
  #Drop items not appeared 30 days before.
  spc_out = spc_out %>% left_join(loop_jxcsdrpt[,.(storecode, prodcode, inv_before = 1)], by = c('storecode','prodcode')) %>% data.table()
  spc_out = spc_out[inv_before == 1]
  #spc_out[,length(unique(prodcode)),by=storecode]
  #spc_out[,length(unique(prodcode))]
  

  #Write spc_out.
  var_write4 = spc_out[,.(storecode, storename, tot_day, catecode, catename, prodcode, oldcode, prodname, prod_invqty, prod_saleqty, prod_daysale, sp_expday, cate_invqty,cate_saleqty,cate_daysale, sc_actexpday, sc_rsnexpday, prodamt, cateamt)]
  var_write4$sp_expday[which(var_write4$sp_expday == Inf)] = "不动销"
  var_write4$sc_actexpday[which(var_write4$sc_actexpday == Inf)] = "不动销"
  var_write4$sc_rsnexpday[which(var_write4$sc_rsnexpday == Inf)] = "不动销"
  var_write4 = var_write4[order(storecode, catecode, prodcode)]
  
  
  var_write4 = data.table(db_group = unik_group[grp], var_write4)
  
  if(dim(var_write4)[1]>0){
    table_4 = rbind(table_4, var_write4)
  }
  

  
  #Compute in-store info.
  spc_in = spc_all[sp_expday < 25 & sc_actexpday < 25]
  spc_in$cateamt = round((pmin(spc_in$sc_rsnexpday, 25) - spc_in$sc_actexpday) * spc_in$cate_daysale)
  spc_in = spc_in[cateamt > 0]
  
  spc_in$prodamt = round((25 - spc_in$sp_expday)*spc_in$prod_daysale)
  spc_in$prodamt = pmin(spc_in$prodamt, spc_in$cateamt)
  
  spc_in = spc_in %>% left_join(stopdist[,.(storecode, prodcode, nodist = 1)], by = c('storecode', 'prodcode')) %>% data.table()
  spc_in = spc_in[is.na(nodist)]
  
  
  
  #Write spc_in.
  var_write5 = spc_in[,.(storecode, storename, tot_day, catecode, catename, prodcode, oldcode, prodname, prod_invqty, prod_saleqty, prod_daysale, sp_expday, cate_invqty,cate_saleqty,cate_daysale, sc_actexpday, sc_rsnexpday, prodamt, cateamt)]
  spc_in$cateamt = -spc_in$cateamt 
  spc_in$prodamt = -spc_in$prodamt 
  var_write5$sc_rsnexpday[which(var_write5$sc_rsnexpday == Inf)] = "不动销"
  var_write5 = var_write5[order(storecode, catecode, prodcode)]
  var_write5 = data.table(db_group = unik_group[grp], var_write5)
  
  if(dim(var_write5)[1]>0){
    table_5 = rbind(table_5, var_write5)
  }
  
  
  
  
  #Put together in&out info.
  if(dim(spc_out)[1]>0 & dim(spc_in)[1]>0){
    in_out = rbind(spc_out[,.(storecode, catecode, prodcode, prodamt, cateamt, prod_invqty)], spc_in[,.(storecode, catecode, prodcode, prodamt, cateamt, prod_invqty = 0)])
    in_out = in_out[prodamt != 0]
    
    #Start computing shipments.
    in_out$storecode = paste('str', in_out$storecode, sep='')
  }else{
    in_out = data.table()
  }


  if(dim(in_out)[1]>0){
    #Check supply & demand for each product, and drop products with no match.
    input_table = data.table(dcast(in_out, prodcode ~ storecode, value.var = 'prodamt', fill = 0))
    
    input_table$tot_need = NA
    for(i in 1:dim(input_table)[1]){
      loop_seq = as.numeric(input_table[i, 2:(dim(input_table)[2]-1)])
      loop_extra = sum(loop_seq[which(sign(loop_seq)==1)])
      loop_short = abs(sum(loop_seq[which(sign(loop_seq)==-1)]))
      input_table$tot_need[i] = min(loop_extra, loop_short)
    }
    input_table = input_table[tot_need != 0]
    all_need = sum(input_table$tot_need)
    
    temp = input_table
    temp$tot_need = NULL
    temp = melt(temp, id = 'prodcode', 
                variable.name = "storecode", 
                value.name = 'prodamt')
    temp = temp[prodamt != 0, .(storecode, prodcode, prodamt)]
    temp$storecode = as.character(temp$storecode)
    
    in_out = temp %>%
      left_join(in_out, by = c('storecode' = 'storecode', 'prodcode' = 'prodcode')) %>%
      data.table()
    in_out = in_out[,.(storecode, catecode, prodcode, prodamt = prodamt.x, cateamt, prod_invqty)]
  
  }
  #Create input_table, input_catetable, pc_table and cate_idx_list.
  if(dim(in_out)[1]>0){
    input_table = data.table(dcast(in_out, prodcode ~ storecode, value.var = 'prodamt', fill = 0))
    
    inv_table = data.table(dcast(in_out, prodcode ~ storecode, value.var = 'prod_invqty', fill = 0))
    
    pc_table = input_table[,.(prodcode)] %>% 
      left_join(in_out[, .(1), by = .(prodcode, catecode)], by = 'prodcode') %>%
      data.table()
    
    input_catetable = data.table(dcast(in_out[, .(1), by = .(storecode, catecode, cateamt)] , catecode ~ storecode, value.var = 'cateamt', fill = 0))

    cate_idx_list = list()
    for(i in 1:length(input_catetable$catecode)){
      cate_idx_list[[i]] = which(pc_table$catecode == input_catetable$catecode[i])
    }
    
    #Compute input in matrix form.
    sdmatx = input_table
    sdmatx$prodcode = NULL
    sdmatx = as.matrix(sdmatx)
    
    cate_sdmatx = input_catetable
    cate_sdmatx$catecode = NULL
    cate_sdmatx = as.matrix(cate_sdmatx)
    
    invmatx = inv_table
    invmatx$prodcode = NULL
    invmatx = as.matrix(invmatx)
    
    compare_table = data.table(expand.grid(from = 1:dim(sdmatx)[2], to = 1:dim(sdmatx)[2]))
    compare_table = compare_table[from != to]
    compare_table = compare_table[order(from, to)]
    compare_table$amt_ship = NA

    #Compute shipments for all posibilities.
    for(i in 1:dim(compare_table)[1]){
      loop_ship = compute_prodshipment(sdmatx[,compare_table$from[i]], sdmatx[,compare_table$to[i]], cate_sdmatx[,compare_table$from[i]], cate_sdmatx[,compare_table$to[i]])
      compare_table$amt_ship[i] = sum(loop_ship)
    }

    bstship = compare_table[which.max(compare_table$amt_ship)[1]]
    bstship$fill_pct = round(bstship$amt_ship/all_need*100, 2)
    
    bst_amt = compute_prodshipment(sdmatx[,bstship$from], sdmatx[,bstship$to], cate_sdmatx[,bstship$from], cate_sdmatx[,bstship$to])
    next_bstship = cbind(bstship, matrix(bst_amt, nrow = 1))
    result_table = next_bstship
    
    #Update sdmatx & cate_matx.
    sdmatx = updt_sdmatx(sdmatx, result_table)
    cate_sdmatx = updt_cate_sdmatx(cate_sdmatx, result_table)
    
    #Update compare_table.
    idx = which(compare_table$from %in% c(next_bstship$from,next_bstship$to) | compare_table$to %in% c(next_bstship$from,next_bstship$to))
    compare_table$amt_ship[idx] = NA

    for(i in idx){
      loop_ship = compute_prodshipment(sdmatx[,compare_table$from[i]], sdmatx[,compare_table$to[i]], cate_sdmatx[,compare_table$from[i]], cate_sdmatx[,compare_table$to[i]])
      compare_table$amt_ship[i] = sum(loop_ship)
    }
    
    
    num_iter = 2
    while(num_iter <= 1000 & max(compare_table$amt_ship) >= min_allship){
      
      bstship = compare_table[which.max(compare_table$amt_ship)[1]]
      bstship$fill_pct = round(bstship$amt_ship/all_need*100, 2)
      bst_amt = compute_prodshipment(sdmatx[,bstship$from], sdmatx[,bstship$to], cate_sdmatx[,bstship$from], cate_sdmatx[,bstship$to])
      next_bstship = cbind(bstship, matrix(bst_amt, nrow = 1))
      result_table = rbind(result_table, next_bstship)
      
      sdmatx = updt_sdmatx(sdmatx, result_table)
      cate_sdmatx = updt_cate_sdmatx(cate_sdmatx, result_table)
      
      idx = which(compare_table$from %in% c(next_bstship$from,next_bstship$to) | compare_table$to %in% c(next_bstship$from,next_bstship$to))
      compare_table$amt_ship[idx] = NA
      
      for(i in idx){
        loop_ship = compute_prodshipment(sdmatx[,compare_table$from[i]], sdmatx[,compare_table$to[i]], cate_sdmatx[,compare_table$from[i]], cate_sdmatx[,compare_table$to[i]])
        compare_table$amt_ship[i] = sum(loop_ship)
      }
      num_iter = num_iter + 1
    }

    #Ship when only few items left.
    unik_from = unique(result_table$from)
    
    for(i in 1:length(unik_from)){
      idx = which(result_table$from == unik_from[i])
      result_table = as.data.frame(result_table)
      
      inv_vct = invmatx[, unik_from[i]]
      ship_vct = as.numeric(colSums(result_table[idx, -(1:4)]))
      inv_vct[which(ship_vct == 0)] = 0
      remain_vct = inv_vct - ship_vct
      remain_idx = which(remain_vct > 0 & remain_vct <= toofewtokeep)
      remain_val = remain_vct[remain_idx]
      

      if(length(remain_idx) > 0){
        before_dt = result_table[idx, c(1,remain_idx + 4)]
        for(kk in 2:(length(remain_val)+1)){
          loop_idx = which(before_dt[,kk] > 0)[1]
          before_dt[,kk][loop_idx] = before_dt[,kk][loop_idx] + remain_val[kk-1]
        }
        result_table[idx, c(1,remain_idx + 4)] = before_dt
      }
      result_table = data.table(result_table)
      result_table$amt_ship = rowSums(result_table[,-1:-4])
    }
    
    #Write summary_table & shipment_detail.
    result_table = result_table[amt_ship >= min_allship]
    
    if(dim(result_table)[1]>0){
      summary_table = result_table[,.(from, to, amt_ship, fill_pct, tot_amt = cumsum(amt_ship), tot_pct = cumsum(fill_pct))]
      str_names = substring(names(input_table)[-1], 4, 9)
      summary_table$from = str_names[summary_table$from]
      summary_table$to = str_names[summary_table$to]
      
      summary_table = summary_table %>% 
        left_join(store_info[,.(storecode, fromstore = storename)], by = c('from' = 'storecode')) %>%
        left_join(store_info[,.(storecode, tostore = storename)], by = c('to' = 'storecode')) %>% 
        data.table()
      summary_table = summary_table[,.(from, fromstore, to, tostore, amt_ship, fill_pct, tot_amt, tot_pct)]
      
      
      summary_table = data.table(db_group = unik_group[grp], summary_table)
      names(summary_table) = c("db_group","from_storecode","from_strorename","to_storecode","to_storename","amt_ship","fill_pct","tot_amt","tot_pct")
      if(dim(summary_table)[1]>0){
        table_6 = rbind(table_6, summary_table)
      }

      shipment_detail = result_table
      shipment_detail$amt_ship = NULL
      shipment_detail$fill_pct = NULL
      
      names(shipment_detail) = c("from", "to",input_table$prodcode)
      shipment_detail$from = str_names[shipment_detail$from]
      shipment_detail$to = str_names[shipment_detail$to]
      shipment_detail$shipment_no = 1:dim(shipment_detail)[1]
      
      shipment_detail <- melt(shipment_detail, id = c("from", "to", "shipment_no"),
                              variable.name = "prodcode", value.name = 'shipment_amt')
      shipment_detail = shipment_detail[order(shipment_no, prodcode)]
      shipment_detail = shipment_detail[shipment_amt != 0]
      
      shipment_detail$prodcode = as.character(shipment_detail$prodcode)
      shipment_detail = shipment_detail %>% 
        left_join(prod_info, by = 'prodcode') %>% 
        left_join(store_info[,.(storecode, fromstore = storename)], by = c('from' = 'storecode')) %>%
        left_join(store_info[,.(storecode, tostore = storename)], by = c('to' = 'storecode')) %>%
        data.table()
      
      
      shipment_detail = shipment_detail[, .(shipment_no, from, fromstore, to, tostore, prodcode, oldcode, prodname, shipment_amt)]
      
      shipment_detail = data.table(db_group = unik_group[grp], shipment_detail)
      names(shipment_detail) = c("db_group","shipment_no","from_storecode","from_strorename","to_storecode","to_storename","prodcode","oldcode","prodname","shipment_amt")

      
      if(dim(shipment_detail)[1]>0){
        table_7 = rbind(table_7, shipment_detail)
      }
      
      
    }


  }
    
}


print(Sys.time())



library('RJDBC')
#database connect
drv <- JDBC("oracle.jdbc.OracleDriver", classPath="/xgm/jdbc_driver/ojdbc6.jar")
dbconn2 <- dbConnect(drv, "jdbc:oracle:thin:@//192.168.1.41:1521/jdpos", "jidiao", "jidiao")



#dbGetQuery(dbconn2, 'SELECT * FROM YDB_MDZT')  %>% data.table()
#dbGetQuery(dbconn2, 'SELECT * FROM YDB_XZXL')  %>% data.table()
#dbGetQuery(dbconn2, 'SELECT * FROM YDB_MDXL')  %>% data.table()
#dbGetQuery(dbconn2, 'SELECT * FROM YDB_OUT')   %>% data.table()
#dbGetQuery(dbconn2, 'SELECT * FROM YDB_IN')    %>% data.table()
#dbGetQuery(dbconn2, 'SELECT * FROM YDB_SUMY')  %>% data.table()
#dbGetQuery(dbconn2, 'SELECT * FROM YDB_ORDER') %>% data.table()


dbSendUpdate(dbconn2, 'DELETE FROM YDB_MDZT')
dbSendUpdate(dbconn2, 'DELETE FROM YDB_XZXL')
dbSendUpdate(dbconn2, 'DELETE FROM YDB_MDXL')
dbSendUpdate(dbconn2, 'DELETE FROM YDB_OUT')
dbSendUpdate(dbconn2, 'DELETE FROM YDB_IN')
dbSendUpdate(dbconn2, 'DELETE FROM YDB_SUMY')
dbSendUpdate(dbconn2, 'DELETE FROM YDB_ORDER')

names(table_2) = toupper(names(table_2))
table_2$CATE_EXPDAY = as.character(table_2$CATE_EXPDAY)
table_3$cate_expday = as.character(table_3$cate_expday)
print(Sys.time())
dbWriteTable(dbconn2, name = "YDB_MDZT",  value = table_1, append = T,row.names = FALSE)
print(dim(table_1)[1])
print(Sys.time())
dbWriteTable(dbconn2, name = "YDB_XZXL",  value = table_2, append = T,row.names = FALSE)
print(dim(table_2)[1])
print(Sys.time())
dbWriteTable(dbconn2, name = "YDB_MDXL",  value = table_3, append = T,row.names = FALSE)
print(dim(table_3)[1])
print(Sys.time())
dbWriteTable(dbconn2, name = "YDB_OUT",   value = table_4, append = T,row.names = FALSE)
print(dim(table_4)[1])
print(Sys.time())
dbWriteTable(dbconn2, name = "YDB_IN",    value = table_5, append = T,row.names = FALSE)
print(dim(table_5)[1])
print(Sys.time())
dbWriteTable(dbconn2, name = "YDB_SUMY",  value = table_6, append = T,row.names = FALSE)
print(dim(table_6)[1])
print(Sys.time())
dbWriteTable(dbconn2, name = "YDB_ORDER", value = table_7, append = T,row.names = FALSE)
print(dim(table_7)[1])
print(Sys.time())


