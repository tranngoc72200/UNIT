	CREATE OR REPLACE PROCEDURE SP_CALCULATE_PAYMENT_PARTNER
	(   
		p_cutoff_date date, 
		p_partner_code varchar2, -- multiple partner: 'FC,BI,FCC'
		p_username varchar2 
	)
	/*
	author: trongcv
	desc: Store t󺀠payment detail/payment summary theo partner, theo k? cutoff
	date: 2025-03-10:
	
			- ĐỐI VỚI PARTNER THÌ K CẦN TÍNH THUẾ/THANH TOÁN FULL CHO PARTNER/ THUẾ CHỈ CÓ Ở PAYMENT DETAIL
	*/
	AS
		v_log_function			varchar2(100);
		v_log_summary_begin		varchar2(1000);
		v_log_summary_end		varchar2(1000);
		v_log_detail_begin		varchar2(1000);
		v_log_exception			nvarchar2(2000);
		v_id                    number(20);
		v_sql					nvarchar2(2000);
		v_channel               varchar2(100);
		v_cutoff_type			varchar2(50);
		v_prev_cutoff_midmonth  date;
	BEGIN

		v_log_function := $$plsql_unit;
		v_log_summary_begin := 'Begin ' || v_log_function || ': '|| to_char(sysdate, 'yyyy-MM-dd hh:mi:ss');

		DELETE TEMP_TABLE_COMMON;
		INSERT INTO TEMP_TABLE_COMMON(key, STRING1)
		WITH RWS AS (
			SELECT P_PARTNER_CODE STR FROM DUAL
		)
		SELECT 'PARTNER' ,  TRIM(REGEXP_SUBSTR (STR, '[^,]+',1,LEVEL)) VALUE
		FROM RWS
		CONNECT BY LEVEL <= LENGTH ( STR ) - LENGTH ( REPLACE ( STR, ',' )) + 1;

	  
		delete JCCE_PAYMENT_DETAIL where PAY_DATE = p_cutoff_date
			and (p_partner_code is null 
				or partner_code in (select string1 from TEMP_TABLE_COMMON where key = 'PARTNER')
			);

		delete JCCE_PAYMENT_SUMMARY where PAY_DATE = p_cutoff_date
			and (p_partner_code is null 
				or partner_code in (select string1 from TEMP_TABLE_COMMON where key = 'PARTNER')
			);
		
		---------------------------------- insert bonus ----------------------------------
		begin
			INSERT INTO JCCE_PAYMENT_DETAIL (CUTOFF_DATE , CALCULATE_DATE , PAY_DATE , CHANNEL , CHANNEL_NAME , PARTNER_CODE , PARTNER_NAME 
				, TYPE_CODE ,  BONUS_CODE , TAX_NONTAX , AMOUNT 
				, DATA_TYPE , REF_ID 
				, CREATED_BY , CREATED_DATE )
		   SELECT 
			A.CUTOFF_DATE, 
			A.CALCULATE_DATE, 
			p_cutoff_date PAY_DATE,
			A.CHANNEL, 
			B.CHANNEL_NAME, 
			A.PARTNER_CODE, 
			NULL PARTNER_NAME, --D.NAME PARTNER_NAME,
			B.BONUS_TYPE TYPE_CODE,
			B.CODE BONUS_CODE, 
			NULL TAX_NONTAX,--C.TAX_NONTAX,
			round(cast(A.RESULT_VALUE as number(18,2)), 0) AS AMOUNT,  
			'SYSTEM' DATA_TYPE, 
			'' REF_ID, 
			p_username CREATED_BY, 
			SYSDATE CREATED_DATE
		FROM JCCE_CNB_COMPENSATIONS_DETAIL_HIST A
			--Lấy thông tin khoản thưởng
			INNER JOIN JCCE_CNB_INFO B ON A.RESULT_CODE = B.CODE  
				AND B.CHANNEL = A.CHANNEL
				AND A.PARTNER_CODE = B.PARTNER_CODE
				AND B.CNB_TYPE = 2 -- partner
			-- Lấy thông tin tax-nontaxable, type group
	--        INNER JOIN jcce_cnb_calendar T ON A.RESULT_CODE = T.CNB_CODE 
	--            AND a.cutoff_date = t.cutoff_date 
	--            and a.calculate_date = t.cutoff_date
	--            and upper(t.result_type) = 'FINAL'
			LEFT JOIN TEMP_TABLE_COMMON e ON e.key = 'PARTNER' 
				And b.partner_code = e.string1
			WHERE 
				(b.partner_code = e.string1 OR p_partner_code IS NULL)
				 AND
				 a.calculate_date = p_cutoff_Date
			;
		end;    
		---------------------------------- end insert bonus ----------------------------------

		---------------------------------- insert commission ----------------------------------
		begin
			INSERT INTO JCCE_PAYMENT_DETAIL (CUTOFF_DATE , CALCULATE_DATE , PAY_DATE , CHANNEL , CHANNEL_NAME , PARTNER_CODE , PARTNER_NAME 
				, TYPE_CODE , AMOUNT, POLICY_NO, DATA_TYPE , REF_ID 
				, CREATED_BY , CREATED_DATE)
			 with list_com as
			 (
				SELECT jci.PARTNER_CODE, JCI.CHANNEL_NAME, CNB_TYPE 
				FROM jcce_cnb_info jci
				INNER JOIN jcce_cnb_calendar jcc ON jci.code = jcc.cnb_code
				LEFT JOIN temp_table_common ttc ON ttc.KEY = 'PARTNER' 
					AND jcc.partner_code = ttc.string1
			   INNER JOIN jcce_mapping_gl gl ON jci.bonus_TYPE = gl.TYPE AND jcc.channel = gl.channel   
			   WHERE gl.TYPE_GROUP = 'COMMISSION'
				   AND (jcc.partner_code = ttc.string1 OR p_partner_code IS NULL)
				   AND jcc.cutoff_date = p_cutoff_date
				   AND jci.CNB_TYPE = 2 -- partner
			 )
			 , COMM as (
                select T1.*, T2.CHANNEL_NAME
					, CASE WHEN FREQUENCY = '00' THEN 'SG_COM' WHEN COMPONENT_YEAR = 1 THEN 'FY_COM' WHEN COMPONENT_YEAR > 1 THEN 'RY_COM' END BONUS_TYPE
				 FROM jcce_commission_detail t1
				 INNER JOIN list_com T2 ON T1.PARTNER_CODE = T2.PARTNER_CODE
				 where pay_date = p_cutoff_Date
			 )
			 SELECT  A.CUTOFF_DATE, 
				A.CUTOFF_DATE CALCULATE_DATE, 
				max(p_cutoff_date) PAY_DATE,
				A.CHANNEL, 
				A.CHANNEL_NAME, 
				A.PARTNER_CODE, 
				NULL PARTNER_NAME,-- D.NAME PARTNER_NAME,
				A.BONUS_TYPE TYPE_CODE,
				SUM(A.COM_AMOUNT) AMOUNT, 
				null POLICY_NO, -- partner không cần detail theo policy
				'SYSTEM' DATA_TYPE, 
				'' REF_ID, 
				MAX(p_username) CREATED_BY, 
				SYSDATE CREATED_DATE
			FROM COMM A
			GROUP BY   A.CUTOFF_DATE, 
				A.CHANNEL, 
				A.CHANNEL_NAME, 
				A.PARTNER_CODE, 
				A.BONUS_TYPE
			 ;
		 end;
		---------------------------------- end Insert commission AGENT ----------------------------------

		---------------------------------- Lấy dữ liệu Adhoc adhoc ----------------------------------
		begin
			INSERT INTO JCCE_PAYMENT_DETAIL (
				CUTOFF_DATE, CALCULATE_DATE, PAY_DATE, CHANNEL, CHANNEL_NAME, 
				PARTNER_CODE, PARTNER_NAME, TYPE_CODE, 
				TYPE_NAME, TYPE_GROUP, BONUS_CODE, TAX_NONTAX, AMOUNT, 
				TAX_AMOUNT, L1, L2, L3, L4, L5, L6, L7, L8, L9, L10, RFA_NO, 
				MEMO_NO, MEMO_NAME, SCHEME_NO, EPAYMENT_NO, POLICY_NO, NOTE, DATA_TYPE, REF_ID, 
				CREATED_BY, CREATED_DATE
			)
			SELECT 
				t1.cutoff_date, 
				t1.cutoff_date CALCULATE_DATE, 
				p_cutoff_Date PAY_DATE,
				t1.CHANNEL,
				t1.CHANNEL_NAME,
				t1.PARTNER_CODE,
				t2.name PARTNER_NAME,
				t1.TYPE_CODE,
				NULL TYPE_NAME, --t4.TYPE_NAME,
				t1.TYPE_GROUP,
				NULL BONUS_CODE,
				NULL TAX_NONTAX,--t4.TAX_NONTAX,
				t1.AMOUNT,
				NULL TAX_AMOUNT,
				t1.L1, t1.L2, t1.L3, t1.L4, t1.L5, t1.L6, t1.L7, t1.L8, t1.L9, t1.L10,
				t1.RFA_NO,
				t1.MEMO_NO,
				t1.MEMO_NAME,
				t1.SCHEME_NO,
				t1.EPAYMENT_NO,
				null POLICY_NO,-- 20/03: do poicy đang cho nhập multi 
				t1.NOTE,
				'ADHOC' DATA_TYPE,
				t1.id REF_ID,
				p_username CREATED_BY,
				SYSDATE CREATED_DATE
			FROM  
				JCCE_ADHOC_INCOME_DEDUCT t1
			LEFT JOIN 
				dms_partner t2 
				ON t1.partner_code = t2.code 
			WHERE 
				t1.cutoff_date = p_cutoff_Date
				and (p_partner_code is null 
				or t1.partner_code in (select string1 from TEMP_TABLE_COMMON where key = 'PARTNER')
				and t1.agent_code is null
			);
		end;
		---------------------------------- end Lấy dữ liệu Adhoc adhoc ----------------------------------

		---------------------------------- Lấy dữ liệu holding compensations  ----------------------------------
		begin
			INSERT INTO JCCE_PAYMENT_DETAIL (CUTOFF_DATE , CALCULATE_DATE , PAY_DATE , CHANNEL , CHANNEL_NAME , PARTNER_CODE , PARTNER_NAME 
				, TYPE_CODE ,AMOUNT , DATA_TYPE , REF_ID 
				, CREATED_BY , CREATED_DATE)
			WITH data_pay as(
				SELECT partner_code, type_code, SUM(total_amount) total_amount
				FROM(
					SELECT 
						pd.partner_code, 
						pd.type_code,  
						SUM(pd.amount) total_amount
					FROM 
						JCCE_PAYMENT_DETAIL pd
					WHERE 
						pd.pay_date = p_cutoff_Date
						and pd.agent_code is null 
					GROUP BY 
						pd.partner_code, pd.type_code
				 )
				 GROUP BY partner_code, type_code
			)
			SELECT 
				p_cutoff_date CUTOFF_DATE,
				p_cutoff_date CALCULATE_DATE,
				p_cutoff_date PAY_DATE,
				h.channel CHANNEL,
				h.channel_name CHANNEL_NAME,
				h.partner_code PARTNER_CODE,
				NULL PARTNER_NAME,--DP.NAME PARTNER_NAME,
				h.type_code TYPE, --gl.type TYPE,
				CASE 
					WHEN h.VALUE_TYPE = '02' THEN H.hold_value 
					ELSE round(case when NVL(pd.total_amount, 0) > 0 then NVL(pd.total_amount, 0) * h.hold_value / 100  else 0 end,0)
				END * (-1) AMOUNT,
				 'HOLD_COMP' DATA_TYPE,
				h.id REF_ID,
				p_username CREATED_BY,
				SYSDATE CREATED_DATE
			FROM
				jcce_agent_holding h
			LEFT JOIN 
				data_pay pd 
				ON h.partner_code = pd.partner_code AND pd.type_code = h.type_code
			WHERE 
				p_cutoff_Date >= h.start_date 
				and ((p_cutoff_Date <= NVL(h.end_date, to_date('99990101', 'yyyyMMdd')) and release_date is null)
					or 
					(p_cutoff_Date < release_date))
				AND h.holding_type = '02'
				AND h.AGENT_CODE IS NULL -- HOLD PARTNER	
				and (p_partner_code is null 
					or h.partner_code in (select string1 from TEMP_TABLE_COMMON where key = 'PARTNER'))
				; -- hold comp
		end;
		
		---------------------------------- end Lấy dữ liệu  holding compensations  ----------------------------------
			
		---------------------------------- release comp ----------------------------------
		
		begin
			INSERT INTO JCCE_PAYMENT_DETAIL (CUTOFF_DATE , CALCULATE_DATE , PAY_DATE , CHANNEL , CHANNEL_NAME , PARTNER_CODE , PARTNER_NAME 
				, TYPE_CODE , AMOUNT , TAX_RATE , TAX_AMOUNT 
				, DATA_TYPE , REF_ID 
				, CREATED_BY , CREATED_DATE)   
				
			select t1.cutoff_date, t1.cutoff_date  CALCULATE_DATE
				, t1.cutoff_date PAY_DATE , t1.CHANNEL , t1.CHANNEL_NAME , t1.PARTNER_CODE
				, NULL PARTNER_NAME --D.name PARTNER_NAME 
				, T2.TYPE_CODE 
				
				, t1.AMOUNT 
				, NULL TAX_RATE --C.TAX_RATE 
				, NULL TAX_AMOUNT --T2.TOTAL_HOLD_AMOUNT * C.TAX_RATE / 100  TAX_AMOUNT 
				, 'RELEASE_COMP' DATA_TYPE , t1.ID REF_ID 
				, p_username CREATED_BY , sysdate CREATED_DATE
			from JCCE_AGENT_RELEASE t1
			inner join JCCE_AGENT_HOLDING t2 on t1.HOLD_REF_ID = t2.id
			where t1.cutoff_date = p_cutoff_date
				and t2.HOLDING_TYPE = '02' -- hold comp
				AND t2.agent_code is null 
				and (p_partner_code is null 
					or t1.partner_code in (select string1 from TEMP_TABLE_COMMON where key = 'PARTNER'))
				; 
		end;
		---------------------------------- end release comp ----------------------------------

		----------------------------------/* cập nhật thông tin payment detail ----------------------------------
		begin 
		MERGE INTO JCCE_PAYMENT_DETAIL T1
		USING (
			 SELECT jpd.ID
				, C.TAX_RATE, C.TYPE_GROUP, C.TYPE_NAME, C.TAX_NONTAX, C.CONTRACT_TYPE
				, dp.NAME PARTNER_NAME
				, C.TYPE_NAME TYPE_DESC
				, C.TYPE_GROUP_NAME
				, C.KPI_TYPE 
			FROM JCCE_PAYMENT_DETAIL jpd
			LEFT JOIN DMS_PARTNER DP 
				ON jpd.PARTNER_CODE = DP.CODE
			LEFT JOIN TEMP_TABLE_COMMON e 
				ON e.key = 'PARTNER' 
				And jpd.partner_code = e.string1
			LEFT JOIN JCCE_MAPPING_GL C 
				ON jpd.type_code = C.TYPE 
				AND jpd.CHANNEL = C.CHANNEL    
			WHERE jpd.pay_date = P_CUTOFF_DATE
				AND (jpd.partner_code = e.string1 OR p_partner_code IS NULL)
		)T2
		ON (T1.ID = T2.ID)
		WHEN MATCHED THEN UPDATE SET 
			 T1.PARTNER_NAME = NVL(T1.PARTNER_NAME, T2.PARTNER_NAME)
			, T1.TAX_RATE = NVL(T1.TAX_RATE, T2.TAX_RATE)/100 -- CASE WHEN NVL(T1.TAX_NONTAX, T2.TAX_NONTAX) = 'Taxable' THEN  NVL(T1.TAX_RATE, T2.TAX_RATE)/100 ELSE 0 END
			, T1.TYPE_GROUP = NVL(T1.TYPE_GROUP, T2.TYPE_GROUP)
			, T1.TYPE_GROUP_NAME = NVL(T1.TYPE_GROUP_NAME, T2.TYPE_GROUP_NAME)
			, T1.TYPE_NAME = NVL(T1.TYPE_NAME, T2.TYPE_NAME)
			, T1.TYPE_DESC = T2.TYPE_DESC
			, T1.TAX_NONTAX = NVL(T1.TAX_NONTAX, T2.TAX_NONTAX)
			, T1.CONTRACT_TYPE = NULL --NVL(T1.CONTRACT_TYPE, T2.CONTRACT_TYPE)
			, T1.TAX_AMOUNT = ROUND(CASE WHEN NVL(T1.TAX_NONTAX, T2.TAX_NONTAX) = 'Taxable' THEN  NVL(T1.TAX_RATE, T2.TAX_RATE)/100 ELSE 0 END * T1.AMOUNT, 0)
			, T1.KPI_TYPE = T2.KPI_TYPE	
			;
		end;	
		----------------------------------/* End cập nhật thông tin payment detail ----------------------------------	

		
		----------------------------------/*T󺀠toᮠpayment summary*/----------------------------------
		 
		INSERT INTO JCCE_PAYMENT_SUMMARY(CUTOFF_DATE, PAY_DATE, CHANNEL, CHANNEL_NAME, PARTNER_CODE, PARTNER_NAME
			, BALANCE_FORWARD, PAID, BONUS, ALLOWANCE, COMMISSION, CONTEST,  UPFRONT_FEE
			, OTHER_TAXABLE_INCOME, OTHER_TAXABLE_DEDUCTION, OTHER_NON_TAXABLE_INCOME
			, OTHER_NON_TAXABLE_DEDUCTION, DEBT_CLEARANCE, WOFF, WON
			, TAXABLE_INCOME_M, TAXABLE_INCOME_Y,  TAX_Y, TAX_M, HOLD_PAYMENT_Y, TAX_HOLD_RETURN, CREATED_DATE, CREATED_BY)
		WITH
		prev_cutoff as (
			SELECT partner_code, MAX(cutoff_date) prev_cutoff_date
			FROM jcce_cutoff_calendar t1
			WHERE cutoff_date < p_cutoff_date
			 AND (p_partner_code  IS NULL 
				OR partner_code IN (SELECT string1 FROM temp_table_common WHERE KEY = 'PARTNER')
			)
			and cutoff_type = 'CNB_CUTOFF'
			GROUP BY partner_code
		),
		prev_payment_sum as (
			select  T1.PARTNER_CODE         
				-- Trường hợp tháng tính là tháng 1 thì RESET TAXABLE_INCOME_Y,  TAX_HOLD_Y, TAX_Y
				, CASE WHEN TO_CHAR(P_CUTOFF_DATE, 'MM') = '01' THEN 0 ELSE t1.TAXABLE_INCOME_Y END TAXABLE_INCOME_Y
				--, CASE WHEN TO_CHAR(P_CUTOFF_DATE, 'MM') = '01' THEN 0 ELSE T1.TAX_HOLD_Y END TAX_HOLD_Y
				, CASE WHEN TO_CHAR(P_CUTOFF_DATE, 'MM') = '01' THEN 0 ELSE T1.TAX_Y END TAX_Y 
				--, t1.CONTRACT_TYPE
				, ENDING_BALANCE, PAYMENT_BANK_TRANSFER, HOLD_PAYMENT_Y
			from VW_PAYMENT_SUMMARY t1 --jcce_payment_summary_hist
			inner join prev_cutoff t2 
				on t1.pay_date = t2.prev_cutoff_date 
				 and t1.partner_code = t2.partner_code
		)
		,
		PAYMENT_SUM as (
			SELECT PAY_DATE, CHANNEL, CHANNEL_NAME, PARTNER_CODE, PARTNER_NAME
				--, NVL(tax_rate,0) tax_rate
					, CASE WHEN UPPER(I.type_group) = 'ADVANCE' 			AND I.TAX_NONTAX = 'NonTaxable'	THEN amount ELSE 0 END ADVANCE
					, CASE WHEN UPPER(I.type_group) = 'BONUS'  			AND I.TAX_NONTAX = 'Taxable'	THEN amount ELSE 0 END BONUS
					, CASE WHEN UPPER(I.type_group) = 'ALLOWANCE' 		AND I.TAX_NONTAX = 'Taxable'	THEN amount ELSE 0 END ALLOWANCE
					, CASE WHEN UPPER(I.type_group) = 'COMMISSION'		AND I.TAX_NONTAX = 'Taxable' 	THEN amount ELSE 0 END COMMISSION
					, CASE WHEN UPPER(I.type_group) = 'CONTEST' 			AND I.TAX_NONTAX = 'Taxable'	THEN amount ELSE 0 END CONTEST
					, CASE WHEN UPPER(I.type_group) = 'OTHER_INCOME' 	AND I.TAX_NONTAX = 'Taxable'	THEN I.amount ELSE 0 END OTHER_TAXABLE_INCOME
					, CASE WHEN UPPER(I.type_group) = 'OTHER_DEDUCTION'  AND I.TAX_NONTAX = 'Taxable' 	THEN I.amount ELSE 0 END OTHER_TAXABLE_DEDUCTION
					, CASE WHEN UPPER(I.type_group) = 'UPFRONT_FEE'  AND I.TAX_NONTAX = 'Taxable' 	THEN I.amount ELSE 0 END UPFRONT_FEE

					, CASE WHEN UPPER(I.type_group) = 'OTHER_INCOME' 
								AND  I.TAX_NONTAX = 'NonTaxable'
								THEN I.amount ELSE 0 END OTHER_NON_TAXABLE_INCOME
					, CASE WHEN UPPER(I.type_group) = 'OTHER_DEDUCTION' 
								AND  I.TAX_NONTAX = 'NonTaxable'	
								THEN I.amount ELSE 0 END OTHER_NON_TAXABLE_DEDUCTION 
					, CASE WHEN UPPER(I.type_group) = 'DEBT_CLEARANCE' 
								AND  I.TAX_NONTAX = 'NonTaxable'
								THEN I.amount ELSE 0 END DEBT_CLEARANCE
					, CASE WHEN UPPER(I.type_group) = 'WOFF' 
								AND I.TAX_NONTAX = 'NonTaxable'
								THEN I.amount ELSE 0 END WOFF
					, CASE WHEN UPPER(I.type_group) = 'WON' 
								AND I.TAX_NONTAX = 'NonTaxable'
								THEN I.amount ELSE 0 END WON  
					, CASE WHEN UPPER(I.type_group) in ('BONUS', 'ALLOWANCE','COMMISSION','CONTEST','OTHER_INCOME','OTHER_DEDUCTION')
						AND I.TAX_NONTAX = 'Taxable' THEN amount ELSE 0 END TAXABLE_INCOME_M
					, CASE WHEN UPPER(I.type_group) in ('BONUS', 'ALLOWANCE','COMMISSION','CONTEST','OTHER_INCOME','OTHER_DEDUCTION') 
						AND I.TAX_NONTAX = 'Taxable' THEN TAX_AMOUNT ELSE 0 END TAX_AMOUNT  

			FROM JCCE_PAYMENT_DETAIL I
			WHERE I.PAY_DATE = p_cutoff_date
				AND (p_partner_code IS NULL 
					OR I.partner_code IN (SELECT string1 FROM TEMP_TABLE_COMMON WHERE KEY = 'PARTNER')) 
				and i.type_code is not null 
		 )
		 SELECT T1.PAY_DATE, t1.PAY_DATE, t1.CHANNEL, t1.CHANNEL_NAME, t1.PARTNER_CODE, t1.PARTNER_NAME
				, NVL(MAX(prev.ENDING_BALANCE), 0) 						AS ENDING_BALANCE
				, (-1) * NVL(MAX(prev.PAYMENT_BANK_TRANSFER), 0)		AS PAID
				, SUM(t1.BONUS)                        					AS BONUS
				, SUM(t1.ALLOWANCE)                    					AS ALLOWANCE
				, SUM(t1.COMMISSION)                   					AS COMMISSION
				, SUM(t1.CONTEST)                      					AS CONTEST
				, SUM(T1.UPFRONT_FEE) 									AS UPFRONT_FEE
				, SUM(t1.OTHER_TAXABLE_INCOME)         					AS OTHER_TAXABLE_INCOME
				, SUM(t1.OTHER_TAXABLE_DEDUCTION)      					AS OTHER_TAXABLE_DEDUCTION
				, SUM(t1.OTHER_NON_TAXABLE_INCOME)     					AS OTHER_NON_TAXABLE_INCOME
				, SUM(t1.OTHER_NON_TAXABLE_DEDUCTION)  					AS OTHER_NON_TAXABLE_DEDUCTION
				, SUM(t1.DEBT_CLEARANCE)               					AS DEBT_CLEARANCE
				, SUM(t1.WOFF)                         					AS WOFF
				, SUM(t1.WON)                          					AS WON
				, NVL(SUM(t1.TAXABLE_INCOME_M),0)             TAXABLE_INCOME_M
				, NVL(SUM(t1.TAXABLE_INCOME_M),0) + NVL(MAX(PREV.TAXABLE_INCOME_Y),0) TAXABLE_INCOME_Y
				, 0 TAX_Y  
				, 0 AS TAX_M  
				, nvl(max(prev.HOLD_PAYMENT_Y),0) HOLD_PAYMENT_Y
				, null TAX_HOLD_RETURN
				, SYSDATE CREATED_DATE
				, MAX(p_username) CREATED_BY
		 FROM PAYMENT_SUM t1
		 LEFT JOIN prev_payment_sum prev 
			ON T1.PARTNER_CODE = prev.PARTNER_CODE
			
		 GROUP BY t1.PAY_DATE, t1.CHANNEL, t1.CHANNEL_NAME, t1.PARTNER_CODE, t1.PARTNER_NAME
				;

		/*cập nhật INCOME_AFTER_TAX, NET_INCOME, ENDING_BALANCE, PAYMENT_BANK_TRANSFER*/

	
		merge into JCCE_PAYMENT_SUMMARY t1
		using (
			select id, NVL(TAXABLE_INCOME_M, 0) - NVL(TAX_M,0) INCOME_AFTER_TAX
				, NVL(TAXABLE_INCOME_M, 0) - NVL(TAX_M, 0) + NVL(OTHER_NON_TAXABLE_DEDUCTION, 0) + NVL(OTHER_NON_TAXABLE_INCOME, 0) 
					 + NVL(WON, 0) + NVL(WOFF, 0) NET_INCOME
			from JCCE_PAYMENT_SUMMARY 
			where PAY_DATE = p_cutoff_date
			and (p_partner_code is null 
				or partner_code in (select string1 from TEMP_TABLE_COMMON where key = 'PARTNER')
			)
		) t2 on (t1.ID = t2.ID)
		when matched then update  set T1.INCOME_AFTER_TAX = T2.INCOME_AFTER_TAX 
			, T1.NET_INCOME = T2.NET_INCOME
			, T1.ENDING_BALANCE = NVL(T1.BALANCE_FORWARD, 0) + NVL(T1.DEBT_CLEARANCE, 0) 
				+ NVL(PAID, 0) + NVL(CASH_ADVANCE, 0) + NVL(BANK_RETURN, 0) + NVL(T2.NET_INCOME, 0)
		;
		
		
		---------------------------------- Xử lý holding */----------------------------------
		begin
			INSERT INTO JCCE_PAYMENT_DETAIL (CUTOFF_DATE , CALCULATE_DATE , PAY_DATE , CHANNEL , CHANNEL_NAME , PARTNER_CODE , PARTNER_NAME 
				, PERCENTAGE , AMOUNT, DATA_TYPE , REF_ID 
				, CREATED_BY , CREATED_DATE)   
			select t2.pay_date, t2.pay_date  CALCULATE_DATE
				, t2.pay_date PAY_DATE , t1.CHANNEL , t1.CHANNEL_NAME , t1.PARTNER_CODE
				, t2. PARTNER_NAME --D.name PARTNER_NAME 
				, case when t1.value_type = '01' THEN t1.hold_value/100 END PERCENT
				, case when t1.value_type = '02' then t1.hold_value else 
						case when t2.TAXABLE_INCOME_M - nvl(T2.TAX_M,0) > 0 
							then round(t1.HOLD_VALUE * (t2.TAXABLE_INCOME_M - nvl(T2.TAX_M,0)) /100,0)
							ELSE 0  end end AMOUNT 
				, 'HOLD_PARTNER' DATA_TYPE , t1.ID REF_ID 
				, p_username CREATED_BY , sysdate CREATED_DATE
			from  jcce_agent_holding t1
			inner join JCCE_PAYMENT_SUMMARY t2 
				on t1.agent_code = t2.agent_code 
				and t2.pay_date = p_cutoff_date
			INNER JOIN DMS_PARTNER T3 ON T1.PARTNER_CODE = T3.CODE	
			WHERE 
				p_cutoff_Date >= t1.start_date 
				and ((p_cutoff_Date <= NVL(t1.end_date, to_date('99990101', 'yyyyMMdd')) and release_date is null)
					or 
					(p_cutoff_Date < t1.release_date))
				AND t1.holding_type = '01'
				AND (p_partner_code is null 
					or t2.partner_code in (select string1 from TEMP_TABLE_COMMON where key = 'PARTNER'))
				AND T1.AGENT_CODE IS NULL
					;
			-- hold PARTNER
		end;
		----------------------------------/*  end Xử lý holding */----------------------------------

		merge into JCCE_PAYMENT_SUMMARY t1
		using (
			select  jpd.agent_code, jpd.CONTRACT_TYPE, jpd.pay_date
				, SUM(CASE WHEN jpd.data_type = 'HOLD_PARTNER' THEN AMOUNT ELSE 0 END) HOLD_AMOUNT
				, SUM(CASE WHEN jpd.data_type = 'RELEASE_PARTNER' THEN AMOUNT ELSE 0 END) RELEASE_AMOUNT
			from JCCE_PAYMENT_DETAIL jpd 
			where jpd.pay_date = p_cutoff_date
			AND (p_partner_code is null 
				or jpd.partner_code in (select string1 from TEMP_TABLE_COMMON where key = 'PARTNER')) 
			AND jpd.data_type IN( 'HOLD_PARTNER', 'RELEASE_PARTNER')    
			GROUP BY jpd.agent_code, jpd.CONTRACT_TYPE, jpd.pay_date
		) T2 ON (T1.agent_code = T2.agent_code AND T1.CONTRACT_TYPE = T2.CONTRACT_TYPE AND  T1.pay_date = T2.pay_date) 
		WHEN MATCHED THEN UPDATE SET 
			T1.HOLD_PAYMENT_M = T2.HOLD_AMOUNT
			, T1.RELEASE_PAYMENT = T2.RELEASE_AMOUNT
			, T1.HOLD_PAYMENT_Y = NVL(T1.HOLD_PAYMENT_Y,0) - NVL(T2.RELEASE_AMOUNT, 0) + NVL(T2.HOLD_AMOUNT,0)
		;
		
		UPDATE JCCE_PAYMENT_SUMMARY SET 
			PAYMENT_BANK_TRANSFER  = CASE WHEN nvl(ENDING_BALANCE,0) - nvl(TAX_HOLD_Y,0) - nvl(HOLD_PAYMENT_Y,0) > 0 
						THEN nvl(ENDING_BALANCE,0) - nvl(TAX_HOLD_Y,0) - nvl(HOLD_PAYMENT_Y,0) ELSE 0 END,
			DEBT_M = CASE WHEN ENDING_BALANCE < 0 THEN ENDING_BALANCE ELSE 0 END
		WHERE PAY_DATE = p_cutoff_date
		AND (p_partner_code is null 
				or partner_code in (select string1 from TEMP_TABLE_COMMON where key = 'PARTNER'));

		/*END cập nhật INCOME_AFTER_TAX, NET_INCOME, ENDING_BALANCE, PAYMENT_BANK_TRANSFER*/


		/* xử lý không có thông tin bank */
		/* END xử lý không có thông tin bank */

		----------------------------------/*END T󺀠toᮠpayment summary*/----------------------------------

	END;
